# PhotoNest

面向 TB 级照片库的家庭相册系统。

核心目标：
- 快速浏览：浏览全程走缓存图（thumb / preview / cover）
- 可控扫描：增量索引 + 限流，不做无脑全量重复扫描
- 权限清晰：管理员发布 + ACL 授权，普通用户仅看被授权项目

## 1. 默认目录约定

- 原图目录（NAS 挂载点，建议只读）: `/mnt/nas/photos`
- 缓存目录（thumb/preview/cover）: `/var/lib/photopanel/cache`
- 数据目录（SQLite）: `/var/lib/photopanel/data`

建议将缓存目录放在 SSD/NVMe。

## 2. 权限模型

角色：
- `admin`
  - 新增/编辑/禁用用户
  - 修改照片路径、缓存路径与扫描参数
  - 启动扫描
  - 发布项目
  - 给项目分配可见用户（ACL）
- `user`
  - 只能访问 `published=true` 且被 ACL 授权的项目
  - 只能下载已授权项目的原图

## 3. 扫描模型（增量 + 限流）

索引表记录：
- `albums`: 项目目录、目录 mtime、发布状态、封面、照片数
- `photos`: 文件相对路径、mtime、size、缓存状态

一次扫描可配置：
- `MAX_SCAN_ALBUMS_PER_RUN`
- `MAX_SCAN_FILES_PER_ALBUM`
- `MAX_NEW_THUMBS_PER_RUN`
- `WORKERS`
- `TIME_BUDGET_SECONDS`
- `PREHEAT_COUNT`（发布后预热数量）

默认策略：
- 目录 mtime 未变化时跳过该目录（增量）
- 发布项目会预热前 N 张
- 用户浏览时按需懒生成缓存

## 4. API 概览

认证：
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/me`

用户侧：
- `GET /api/stats`
- `GET /api/years`
- `GET /api/albums?year=2025&q=关键词`
- `GET /api/photos?album_id=123&page=1&page_size=200`
- `GET /api/cover/{album_id}`
- `GET /api/thumb/{photo_id}`
- `GET /api/preview/{photo_id}`
- `GET /api/download/{photo_id}`

管理侧：
- `GET/PUT /api/admin/settings`
- `GET/POST /api/admin/users`
- `PUT /api/admin/users/{id}`
- `GET /api/admin/albums`
- `POST /api/admin/albums/scan`
- `POST /api/admin/albums/{id}/publish`
- `POST /api/admin/albums/{id}/acl`
- `GET /api/admin/scan-status`

## 5. 安装（Debian/Ubuntu）

使用交互式安装脚本：

```bash
chmod +x install.sh
sudo ./install.sh
```

安装脚本会：
- 安装依赖（git/python3/venv/pillow 相关库）
- 创建运行用户 `photopanel`
- 从 GitHub 拉取代码：`https://github.com/cyeinfpro/PhotoPanel`
- 创建 venv 并安装 `requirements.txt`
- 交互式输入默认管理员账号/密码
- 生成 `/etc/photopanel.env`
- 写入并启动 `systemd` 服务 `photopanel`
- 输出服务访问端口（Nginx/反向代理由你自行配置）

## 6. 运行与维护

```bash
# 服务状态
systemctl status photopanel

# 实时日志
journalctl -u photopanel -f

# 重启
systemctl restart photopanel
```

## 7. 开发运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export PHOTO_ROOT=/mnt/nas/photos
export CACHE_ROOT=/var/lib/photopanel/cache
export DATA_ROOT=/var/lib/photopanel/data
export PHOTO_DB=/var/lib/photopanel/data/photopanel.db
export SECRET_KEY=replace_me

python3 app.py
```
