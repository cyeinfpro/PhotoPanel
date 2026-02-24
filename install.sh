#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/cyeinfpro/PhotoPanel"
APP_DIR="/opt/PhotoPanel"
RUN_USER="photopanel"
ENV_FILE="/etc/photopanel.env"
SERVICE_FILE="/etc/systemd/system/photopanel.service"

DEFAULT_PHOTO_ROOT="/mnt/nas/photos"
DEFAULT_CACHE_ROOT="/var/lib/photopanel/cache"
DEFAULT_DATA_ROOT="/var/lib/photopanel/data"
DEFAULT_PORT="8080"
DEFAULT_ADMIN_USER="admin"

if [[ "${EUID}" -ne 0 ]]; then
  echo "请使用 root 运行安装脚本"
  exit 1
fi

echo "==============================================="
echo " PhotoNest 安装程序"
echo "==============================================="

PHOTO_ROOT="${PHOTO_ROOT:-${DEFAULT_PHOTO_ROOT}}"
CACHE_ROOT="${CACHE_ROOT:-${DEFAULT_CACHE_ROOT}}"
DATA_ROOT="${DATA_ROOT:-${DEFAULT_DATA_ROOT}}"
PORT="${PORT:-${DEFAULT_PORT}}"

read -r -p "默认管理员账号 [${DEFAULT_ADMIN_USER}]: " ADMIN_USER
if [[ -z "${ADMIN_USER}" ]]; then
  ADMIN_USER="${DEFAULT_ADMIN_USER}"
fi

while true; do
  read -r -s -p "默认管理员密码 (至少6位): " ADMIN_PASSWORD
  echo
  if [[ ${#ADMIN_PASSWORD} -lt 6 ]]; then
    echo "密码长度不足 6 位，请重试"
    continue
  fi
  read -r -s -p "确认管理员密码: " ADMIN_PASSWORD_CONFIRM
  echo
  if [[ "${ADMIN_PASSWORD}" != "${ADMIN_PASSWORD_CONFIRM}" ]]; then
    echo "两次输入不一致，请重试"
    continue
  fi
  break
done

SECRET_KEY="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)"

echo
echo "将使用以下配置："
echo "- REPO_URL:    ${REPO_URL}"
echo "- APP_DIR:     ${APP_DIR}"
echo "- PHOTO_ROOT:  ${PHOTO_ROOT}"
echo "- CACHE_ROOT:  ${CACHE_ROOT}"
echo "- DATA_ROOT:   ${DATA_ROOT}"
echo "- PORT:        ${PORT}"
echo "- ADMIN_USER:  ${ADMIN_USER}"
echo

echo "[1/7] 安装系统依赖..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  git curl python3 python3-venv python3-pip python3-setuptools \
  libjpeg-dev libwebp-dev libheif-dev

echo "[2/7] 创建运行用户和目录..."
id -u "${RUN_USER}" >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -m "${RUN_USER}"
mkdir -p "${PHOTO_ROOT}" "${CACHE_ROOT}" "${DATA_ROOT}"
chown -R "${RUN_USER}:${RUN_USER}" "${CACHE_ROOT}" "${DATA_ROOT}"

echo "[3/7] 拉取/更新代码..."
if [[ ! -d "${APP_DIR}/.git" ]]; then
  rm -rf "${APP_DIR}"
  git clone "${REPO_URL}" "${APP_DIR}"
else
  if ! git -C "${APP_DIR}" fetch --all || ! git -C "${APP_DIR}" pull --ff-only; then
    echo "现有代码更新失败，改为重新拉取代码..."
    TMP_APP_DIR="${APP_DIR}.tmp.$(date +%s)"
    rm -rf "${TMP_APP_DIR}"
    git clone "${REPO_URL}" "${TMP_APP_DIR}"
    rm -rf "${APP_DIR}"
    mv "${TMP_APP_DIR}" "${APP_DIR}"
  fi
fi

echo "[4/7] 安装 Python 依赖..."
python3 -m venv "${APP_DIR}/.venv"
PY_BIN="${APP_DIR}/.venv/bin/python"
PY_MINOR="$("${PY_BIN}" - <<'PY'
import sys
print(sys.version_info.minor)
PY
)"

if (( PY_MINOR < 8 )); then
  "${PY_BIN}" -m pip install -U "pip<24.1" "setuptools<69" wheel
else
  "${PY_BIN}" -m pip install -U pip setuptools wheel
fi

install_fallback_deps() {
  local flask_spec
  local werkzeug_spec

  if (( PY_MINOR >= 10 )); then
    flask_spec="Flask>=2.3,<3.0"
    werkzeug_spec="Werkzeug>=2.3,<3.0"
  else
    flask_spec="Flask>=2.2,<2.3"
    werkzeug_spec="Werkzeug>=2.2,<2.3"
  fi

  "${PY_BIN}" -m pip install \
    "${flask_spec}" \
    "${werkzeug_spec}" \
    "Jinja2>=3.1,<4.0" \
    "itsdangerous>=2.1,<3.0" \
    "Pillow>=9.2,<11.0" \
    "gunicorn>=20.1,<23.0"

  if (( PY_MINOR >= 8 )); then
    if ! "${PY_BIN}" -m pip install "pillow-heif>=0.10.0"; then
      echo "警告: pillow-heif 安装失败，将以无 HEIC 解码模式运行。"
    fi
  fi
}

if [[ -f "${APP_DIR}/requirements.txt" ]]; then
  if ! "${PY_BIN}" -m pip install -r "${APP_DIR}/requirements.txt"; then
    echo "requirements.txt 安装失败，回退到兼容依赖组合..."
    install_fallback_deps
  fi
else
  install_fallback_deps
fi

echo "[5/7] 写入环境文件 ${ENV_FILE} ..."
cat > "${ENV_FILE}" <<EOF
PHOTO_ROOT=${PHOTO_ROOT}
CACHE_ROOT=${CACHE_ROOT}
DATA_ROOT=${DATA_ROOT}
PHOTO_DB=${DATA_ROOT}/photopanel.db
PORT=${PORT}
SECRET_KEY=${SECRET_KEY}
ADMIN_USER=${ADMIN_USER}
ADMIN_PASSWORD=${ADMIN_PASSWORD}
ENABLE_X_ACCEL=0
ACCEL_CACHE_PREFIX=/_cache
ACCEL_ORIG_PREFIX=/_orig
EOF
chmod 600 "${ENV_FILE}"

echo "[6/7] 配置 systemd 服务..."
cat > "${SERVICE_FILE}" <<'EOF'
[Unit]
Description=PhotoNest Service
After=network.target

[Service]
Type=simple
User=photopanel
Group=photopanel
WorkingDirectory=/opt/PhotoPanel
EnvironmentFile=/etc/photopanel.env
ExecStart=/opt/PhotoPanel/.venv/bin/gunicorn \
  --bind 0.0.0.0:${PORT} \
  --workers 4 \
  --threads 4 \
  --timeout 120 \
  app:app
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now photopanel

echo "等待服务启动..."
SERVICE_OK=0
for _ in $(seq 1 20); do
  if curl -fsS "http://127.0.0.1:${PORT}/login" > /dev/null 2>&1; then
    SERVICE_OK=1
    break
  fi
  sleep 1
done

if [[ "${SERVICE_OK}" -ne 1 ]]; then
  echo "服务启动失败，输出最近日志："
  journalctl -u photopanel -n 120 --no-pager || true
  exit 1
fi

echo "[7/7] 完成"
echo
echo "访问地址: http://$(hostname -I | awk '{print $1}'):${PORT}"
echo "默认管理员: ${ADMIN_USER}"
echo "提示: Nginx/反向代理请由你后续自行配置。"
echo "提示: 管理员首次登录后可在后台继续设置照片目录和扫描参数。"
echo "服务状态: systemctl status photopanel"
