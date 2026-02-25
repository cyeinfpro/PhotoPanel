#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/cyeinfpro/PhotoPanel"
APP_DIR="/opt/PhotoPanel"
RUN_USER="root"
RUN_GROUP="root"
ENV_FILE="/etc/photopanel.env"
SERVICE_FILE="/etc/systemd/system/photopanel.service"
SERVICE_NAME="photopanel"

DEFAULT_PHOTO_ROOT="/mnt/nas/photos"
DEFAULT_CACHE_ROOT="/var/lib/photopanel/cache"
DEFAULT_DATA_ROOT="/var/lib/photopanel/data"
DEFAULT_CACHE_ROOT_FALLBACK="/var/lib/photopanel/cache-local"
DEFAULT_DATA_ROOT_FALLBACK="/var/lib/photopanel/data-local"
DEFAULT_PORT="8080"
DEFAULT_ADMIN_USER="admin"

if [[ "${EUID}" -ne 0 ]]; then
  echo "请使用 root 运行安装脚本"
  exit 1
fi

print_header() {
  echo "==============================================="
  echo " PhotoNest 管理脚本"
  echo "==============================================="
}

get_env_value() {
  local key="$1"
  local default_value="${2:-}"
  if [[ -f "${ENV_FILE}" ]]; then
    local line
    line="$(grep -E "^${key}=" "${ENV_FILE}" | tail -n 1 || true)"
    if [[ -n "${line}" ]]; then
      echo "${line#*=}"
      return
    fi
  fi
  echo "${default_value}"
}

safe_rm_rf() {
  local target="$1"
  if [[ -z "${target}" || "${target}" == "/" ]]; then
    echo "跳过危险删除路径: '${target}'"
    return
  fi
  if [[ -e "${target}" ]]; then
    rm -rf "${target}"
  fi
}

safe_rm_f() {
  local target="$1"
  if [[ -n "${target}" && -e "${target}" ]]; then
    rm -f "${target}"
  fi
}

upsert_env_key() {
  local key="$1"
  local value="$2"
  local tmp_file
  tmp_file="$(mktemp)"

  if [[ -f "${ENV_FILE}" ]]; then
    awk -v k="${key}" -v v="${value}" '
      BEGIN { done = 0 }
      {
        if ($0 ~ ("^" k "=")) {
          print k "=" v
          done = 1
        } else {
          print $0
        }
      }
      END {
        if (!done) {
          print k "=" v
        }
      }
    ' "${ENV_FILE}" > "${tmp_file}"
  else
    printf '%s=%s\n' "${key}" "${value}" > "${tmp_file}"
  fi

  mv "${tmp_file}" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
}

install_system_deps() {
  echo "安装系统依赖..."
  apt-get update -y
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git curl python3 python3-venv python3-pip python3-setuptools \
    libjpeg-dev libwebp-dev libheif-dev
}

ensure_runtime_user_and_dirs() {
  local photo_root="$1"
  local cache_root="$2"
  local data_root="$3"

  if [[ "${RUN_USER}" != "root" ]]; then
    id -u "${RUN_USER}" >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -m "${RUN_USER}"
  fi
  mkdir -p "${photo_root}" "${cache_root}" "${data_root}"
  chown -R "${RUN_USER}:${RUN_GROUP}" "${cache_root}" "${data_root}"
  chmod -R u+rwX "${cache_root}" "${data_root}" || true
}

verify_runtime_access() {
  local photo_root="$1"
  local cache_root="$2"
  local data_root="$3"

  if ! command -v runuser >/dev/null 2>&1; then
    echo "缺少 runuser 命令，无法校验运行用户权限"
    return 1
  fi

  echo "校验运行时目录权限（用户: ${RUN_USER}）..."

  if ! runuser -u "${RUN_USER}" -- test -d "${photo_root}"; then
    echo "警告: PHOTO_ROOT 不是目录或不可访问: ${photo_root}"
    echo "提示: 不影响面板启动，但扫描会失败。可在后台设置中后续调整。"
  elif ! runuser -u "${RUN_USER}" -- test -r "${photo_root}" || ! runuser -u "${RUN_USER}" -- test -x "${photo_root}"; then
    echo "警告: PHOTO_ROOT 读/遍历权限不足: ${photo_root}"
    echo "提示: SMB 只读目录是允许的，但需至少可读+可遍历。"
  fi

  if ! runuser -u "${RUN_USER}" -- test -d "${cache_root}"; then
    echo "错误: CACHE_ROOT 不是目录或不可访问: ${cache_root}"
    return 1
  fi
  if ! runuser -u "${RUN_USER}" -- test -w "${cache_root}"; then
    echo "错误: CACHE_ROOT 不可写: ${cache_root}"
    echo "提示: 如果是 SMB 挂载，请在挂载参数中指定 uid/gid=${RUN_USER} 或允许写入。"
    return 1
  fi
  if ! runuser -u "${RUN_USER}" -- bash -lc "mkdir -p '${cache_root}/thumb' '${cache_root}/preview' '${cache_root}/cover'"; then
    echo "错误: 无法在 CACHE_ROOT 创建缓存子目录: ${cache_root}"
    return 1
  fi
  if ! runuser -u "${RUN_USER}" -- bash -lc "probe='${cache_root}/.perm_probe_$$'; echo ok > \"\$probe\" && rm -f \"\$probe\""; then
    echo "错误: CACHE_ROOT 写入探针失败: ${cache_root}"
    return 1
  fi

  if ! runuser -u "${RUN_USER}" -- test -d "${data_root}"; then
    echo "错误: DATA_ROOT 不是目录或不可访问: ${data_root}"
    return 1
  fi
  if ! runuser -u "${RUN_USER}" -- test -w "${data_root}"; then
    echo "错误: DATA_ROOT 不可写: ${data_root}"
    return 1
  fi
  if ! runuser -u "${RUN_USER}" -- bash -lc "probe='${data_root}/.perm_probe_$$'; echo ok > \"\$probe\" && rm -f \"\$probe\""; then
    echo "错误: DATA_ROOT 写入探针失败: ${data_root}"
    return 1
  fi
}

resolve_runtime_paths() {
  local photo_root="$1"
  local cache_root="$2"
  local data_root="$3"
  RESOLVED_CACHE_ROOT="${cache_root}"
  RESOLVED_DATA_ROOT="${data_root}"

  ensure_runtime_user_and_dirs "${photo_root}" "${cache_root}" "${data_root}"
  if verify_runtime_access "${photo_root}" "${cache_root}" "${data_root}"; then
    RESOLVED_CACHE_ROOT="${cache_root}"
    RESOLVED_DATA_ROOT="${data_root}"
    return 0
  fi

  local fallback_cache="${DEFAULT_CACHE_ROOT_FALLBACK}"
  local fallback_data="${DEFAULT_DATA_ROOT_FALLBACK}"
  echo "检测到 cache/data 目录权限异常，自动切换到本地可写目录："
  echo "- CACHE_ROOT => ${fallback_cache}"
  echo "- DATA_ROOT  => ${fallback_data}"

  ensure_runtime_user_and_dirs "${photo_root}" "${fallback_cache}" "${fallback_data}"
  verify_runtime_access "${photo_root}" "${fallback_cache}" "${fallback_data}"
  RESOLVED_CACHE_ROOT="${fallback_cache}"
  RESOLVED_DATA_ROOT="${fallback_data}"
}

sync_repo() {
  if [[ ! -d "${APP_DIR}/.git" ]]; then
    rm -rf "${APP_DIR}"
    git clone "${REPO_URL}" "${APP_DIR}"
    return
  fi

  if ! git -C "${APP_DIR}" fetch --all || ! git -C "${APP_DIR}" pull --ff-only; then
    echo "现有代码更新失败，改为重新拉取代码..."
    local tmp_app_dir="${APP_DIR}.tmp.$(date +%s)"
    rm -rf "${tmp_app_dir}"
    git clone "${REPO_URL}" "${tmp_app_dir}"
    rm -rf "${APP_DIR}"
    mv "${tmp_app_dir}" "${APP_DIR}"
  fi
}

install_fallback_deps() {
  local py_bin="$1"
  local py_minor="$2"
  local flask_spec
  local werkzeug_spec

  if (( py_minor >= 10 )); then
    flask_spec="Flask>=2.3,<3.0"
    werkzeug_spec="Werkzeug>=2.3,<3.0"
  else
    flask_spec="Flask>=2.2,<2.3"
    werkzeug_spec="Werkzeug>=2.2,<2.3"
  fi

  "${py_bin}" -m pip install \
    "${flask_spec}" \
    "${werkzeug_spec}" \
    "Jinja2>=3.1,<4.0" \
    "itsdangerous>=2.1,<3.0" \
    "Pillow>=9.2,<11.0" \
    "gunicorn>=20.1,<23.0"

  if (( py_minor >= 8 )); then
    if ! "${py_bin}" -m pip install "pillow-heif>=0.10.0"; then
      echo "警告: pillow-heif 安装失败，将以无 HEIC 解码模式运行。"
    fi
  fi
}

install_python_deps() {
  local py_bin="${APP_DIR}/.venv/bin/python"
  python3 -m venv "${APP_DIR}/.venv"

  local py_minor
  py_minor="$("${py_bin}" - <<'PY'
import sys
print(sys.version_info.minor)
PY
)"

  if (( py_minor < 8 )); then
    "${py_bin}" -m pip install -U "pip<24.1" "setuptools<69" wheel
  else
    "${py_bin}" -m pip install -U pip setuptools wheel
  fi

  if [[ -f "${APP_DIR}/requirements.txt" ]]; then
    if ! "${py_bin}" -m pip install -r "${APP_DIR}/requirements.txt"; then
      echo "requirements.txt 安装失败，回退到兼容依赖组合..."
      install_fallback_deps "${py_bin}" "${py_minor}"
    fi
  else
    install_fallback_deps "${py_bin}" "${py_minor}"
  fi
}

write_env_file() {
  local photo_root="$1"
  local cache_root="$2"
  local data_root="$3"
  local port="$4"
  local admin_user="$5"
  local admin_password="$6"
  local secret_key="$7"

  cat > "${ENV_FILE}" <<EOF
PHOTO_ROOT=${photo_root}
CACHE_ROOT=${cache_root}
DATA_ROOT=${data_root}
PHOTO_DB=${data_root}/photopanel.db
PORT=${port}
SECRET_KEY=${secret_key}
ADMIN_USER=${admin_user}
ADMIN_PASSWORD=${admin_password}
ENABLE_X_ACCEL=0
ACCEL_CACHE_PREFIX=/_cache
ACCEL_ORIG_PREFIX=/_orig
EOF
  chmod 600 "${ENV_FILE}"
}

write_service_file() {
  cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=PhotoNest Service
After=network.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/.venv/bin/gunicorn \
  --bind 0.0.0.0:\${PORT} \
  --workers 4 \
  --threads 4 \
  --timeout 120 \
  app:app
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
}

wait_for_health() {
  local port="$1"
  local ok=0
  for _ in $(seq 1 25); do
    if curl -fsS "http://127.0.0.1:${port}/login" > /dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 1
  done

  if [[ "${ok}" -ne 1 ]]; then
    echo "服务启动失败，输出最近日志："
    journalctl -u "${SERVICE_NAME}" -n 120 --no-pager || true
    return 1
  fi
}

install_panel() {
  print_header
  echo "操作: 安装面板"
  echo

  local photo_root="${PHOTO_ROOT:-${DEFAULT_PHOTO_ROOT}}"
  local cache_root="${CACHE_ROOT:-${DEFAULT_CACHE_ROOT}}"
  local data_root="${DATA_ROOT:-${DEFAULT_DATA_ROOT}}"
  local port="${PORT:-${DEFAULT_PORT}}"

  read -r -p "默认管理员账号 [${DEFAULT_ADMIN_USER}]: " admin_user
  if [[ -z "${admin_user}" ]]; then
    admin_user="${DEFAULT_ADMIN_USER}"
  fi

  local admin_password
  local admin_password_confirm
  while true; do
    read -r -s -p "默认管理员密码 (至少6位): " admin_password
    echo
    if [[ ${#admin_password} -lt 6 ]]; then
      echo "密码长度不足 6 位，请重试"
      continue
    fi
    read -r -s -p "确认管理员密码: " admin_password_confirm
    echo
    if [[ "${admin_password}" != "${admin_password_confirm}" ]]; then
      echo "两次输入不一致，请重试"
      continue
    fi
    break
  done

  local secret_key
  secret_key="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)"

  echo
  echo "将使用以下配置："
  echo "- REPO_URL:    ${REPO_URL}"
  echo "- APP_DIR:     ${APP_DIR}"
  echo "- PHOTO_ROOT:  ${photo_root}"
  echo "- CACHE_ROOT:  ${cache_root}"
  echo "- DATA_ROOT:   ${data_root}"
  echo "- PORT:        ${port}"
  echo "- ADMIN_USER:  ${admin_user}"
  echo

  echo "[1/7] 安装系统依赖..."
  install_system_deps

  echo "[2/7] 创建运行用户并准备目录..."
  resolve_runtime_paths "${photo_root}" "${cache_root}" "${data_root}"
  cache_root="${RESOLVED_CACHE_ROOT}"
  data_root="${RESOLVED_DATA_ROOT}"

  echo "[3/7] 拉取代码..."
  sync_repo

  echo "[4/7] 安装 Python 依赖..."
  install_python_deps

  echo "[5/7] 写入环境文件 ${ENV_FILE} ..."
  write_env_file "${photo_root}" "${cache_root}" "${data_root}" "${port}" "${admin_user}" "${admin_password}" "${secret_key}"

  echo "[6/7] 写入并启动 systemd 服务..."
  write_service_file
  systemctl daemon-reload
  systemctl enable --now "${SERVICE_NAME}"

  echo "[7/7] 健康检查..."
  wait_for_health "${port}"

  local host_ip
  host_ip="$(hostname -I | awk '{print $1}')"
  echo
  echo "安装完成"
  echo "访问地址: http://${host_ip}:${port}"
  echo "默认管理员: ${admin_user}"
  echo "服务状态: systemctl status ${SERVICE_NAME}"
}

update_panel() {
  print_header
  echo "操作: 更新面板"
  echo

  if [[ ! -d "${APP_DIR}" ]]; then
    echo "未检测到安装目录: ${APP_DIR}"
    echo "请先执行“安装面板”"
    exit 1
  fi

  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "未检测到环境文件: ${ENV_FILE}"
    echo "请先执行“安装面板”或恢复环境文件"
    exit 1
  fi

  local port
  local photo_root
  local cache_root
  local data_root
  port="$(get_env_value PORT "${DEFAULT_PORT}")"
  photo_root="$(get_env_value PHOTO_ROOT "${DEFAULT_PHOTO_ROOT}")"
  cache_root="$(get_env_value CACHE_ROOT "${DEFAULT_CACHE_ROOT}")"
  data_root="$(get_env_value DATA_ROOT "${DEFAULT_DATA_ROOT}")"

  echo "[1/6] 修复运行目录权限..."
  local old_cache_root="${cache_root}"
  local old_data_root="${data_root}"
  resolve_runtime_paths "${photo_root}" "${cache_root}" "${data_root}"
  cache_root="${RESOLVED_CACHE_ROOT}"
  data_root="${RESOLVED_DATA_ROOT}"
  if [[ "${cache_root}" != "${old_cache_root}" || "${data_root}" != "${old_data_root}" ]]; then
    echo "检测到目录自动回退，更新环境文件..."
    upsert_env_key "CACHE_ROOT" "${cache_root}"
    upsert_env_key "DATA_ROOT" "${data_root}"
    upsert_env_key "PHOTO_DB" "${data_root}/photopanel.db"
  fi

  echo "[2/6] 更新代码..."
  sync_repo

  echo "[3/6] 更新 Python 依赖..."
  install_python_deps

  echo "[4/6] 刷新 systemd 服务定义..."
  write_service_file
  systemctl daemon-reload

  echo "[5/6] 重启服务..."
  systemctl restart "${SERVICE_NAME}"

  echo "[6/6] 健康检查..."
  wait_for_health "${port}"

  echo
  echo "更新完成"
  echo "服务状态: systemctl status ${SERVICE_NAME}"
}

uninstall_panel() {
  print_header
  echo "操作: 卸载面板"
  echo "警告: 此操作将删除应用目录、service 和环境文件。"
  echo

  local photo_root
  local cache_root
  local data_root
  photo_root="$(get_env_value PHOTO_ROOT "${DEFAULT_PHOTO_ROOT}")"
  cache_root="$(get_env_value CACHE_ROOT "${DEFAULT_CACHE_ROOT}")"
  data_root="$(get_env_value DATA_ROOT "${DEFAULT_DATA_ROOT}")"

  read -r -p "输入 UNINSTALL 确认卸载: " confirm_word
  if [[ "${confirm_word}" != "UNINSTALL" ]]; then
    echo "已取消"
    exit 0
  fi

  echo "[1/4] 停止并禁用服务..."
  systemctl disable --now "${SERVICE_NAME}" >/dev/null 2>&1 || true

  echo "[2/4] 删除 systemd 服务文件..."
  safe_rm_f "${SERVICE_FILE}"
  systemctl daemon-reload

  echo "[3/4] 删除应用与环境文件..."
  safe_rm_rf "${APP_DIR}"
  safe_rm_f "${ENV_FILE}"

  echo "[4/4] 清理可选数据..."
  echo "- 原图目录将保留不删除: ${photo_root}"
  read -r -p "是否删除缓存与数据库目录 (${cache_root}, ${data_root})? [y/N]: " remove_data
  if [[ "${remove_data}" =~ ^[Yy]$ ]]; then
    safe_rm_rf "${cache_root}"
    safe_rm_rf "${data_root}"
    echo "已删除缓存与数据库目录"
  else
    echo "已保留缓存与数据库目录"
  fi

  read -r -p "是否删除运行用户 ${RUN_USER}? [y/N]: " remove_user
  if [[ "${RUN_USER}" != "root" && "${remove_user}" =~ ^[Yy]$ ]]; then
    userdel -r "${RUN_USER}" >/dev/null 2>&1 || true
    echo "已尝试删除运行用户 ${RUN_USER}"
  elif [[ "${RUN_USER}" == "root" ]]; then
    echo "运行用户为 root，跳过删除用户步骤"
  fi

  echo
  echo "卸载完成"
}

print_header
echo "请选择操作:"
echo "1) 安装面板"
echo "2) 更新面板"
echo "3) 卸载面板"
echo
read -r -p "输入选项 [1/2/3]: " action

case "${action}" in
  1)
    install_panel
    ;;
  2)
    update_panel
    ;;
  3)
    uninstall_panel
    ;;
  *)
    echo "无效选项: ${action}"
    exit 1
    ;;
esac
