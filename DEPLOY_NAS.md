# 群晖 NAS (DS918+) 离线部署方案

由于您的 NAS 处于纯内网环境，无法连接互联网，我们提供两种部署方案。推荐使用 **方案一（Docker 容器化部署）**，因为它最稳定且环境一致。如果不想使用 Docker，可以使用 **方案二（直接 Python 运行）**。

---

## 方案一：Docker 容器化部署（推荐）

此方案将程序和所有依赖打包成一个镜像文件，直接在 NAS 上导入运行，无需在 NAS 上安装 Python 环境。

### 第一步：在有网的电脑上打包镜像
1. 确保电脑上安装了 Docker Desktop。
2. 将项目文件夹 `qiangongshi-New` 复制到电脑上。
3. 打开命令行（CMD 或 PowerShell），进入该文件夹：
   ```bash
   cd F:\qiangongshi-New
   ```
4. 构建镜像（注意最后有个点）：
   ```bash
   docker build -t qiangongshi:v1 .
   ```
5. 将镜像保存为文件（这一步会生成一个约 500MB+ 的文件）：
   ```bash
   docker save -o qiangongshi_v1.tar qiangongshi:v1
   ```
6. 将 `qiangongshi_v1.tar` 文件复制到 U 盘或直接上传到 NAS 的某个文件夹（如 `/volume1/docker/qiangongshi/`）。

### 第二步：在 NAS 上导入并运行
1. 登录群晖 DSM 界面。
2. 打开 **套件中心**，安装 **Container Manager** (旧版本叫 Docker)。
3. 打开 **Container Manager** -> **映像** -> **操作** -> **导入** -> **从文件添加**。
4. 选择刚才上传的 `qiangongshi_v1.tar`，等待导入完成。
5. 在映像列表中找到 `qiangongshi:v1`，点击 **运行**。
6. **配置容器**：
   - **容器名称**：qiangongshi
   - **端口设置**：将容器端口 `5000` 映射到本地端口 `5000` (或任意未被占用的端口)。
   - **存储空间设置**（重要，防止数据丢失）：
     - 点击“添加文件夹”，选择 NAS 上的一个文件夹（如 `/docker/qiangongshi/data`）。
     - 挂载路径填写：`/app/data` (我们需要修改一下代码让数据存放在这里，稍后会提供修改)。
     - 或者简单粗暴点，直接把 `/app` 挂载出来也可以，但可能会覆盖镜像内的代码。
     - **推荐挂载方式**：
       - NAS 文件夹 `/docker/qiangongshi/uploads` -> 挂载路径 `/app/uploads`
       - NAS 文件夹 `/docker/qiangongshi/excel_templates` -> 挂载路径 `/app/excel_templates`
       - NAS 文件 `/docker/qiangongshi/mechanics.db` -> 挂载路径 `/app/mechanics.db` (需要先在 NAS 上创建一个空的或现有的 db 文件)
   - **环境**：保持默认。
7. 点击 **完成**，容器启动。
8. 在浏览器输入 `http://NAS_IP:5000` 即可访问。

---

## 方案二：直接 Python 运行（纯文件拷贝）

此方案需要在有网电脑上下载好所有依赖包（Wheel 文件），然后拷贝到 NAS 上安装。

### 第一步：下载依赖包（在有网电脑上）
1. 确保电脑上安装了 Python (最好是 3.8 或 3.9，与 NAS 版本接近)。
2. 在项目文件夹下创建一个 `packages` 文件夹。
3. 使用 pip 下载 Linux 版本的依赖包（因为 NAS 是 Linux 系统）：
   打开命令行，运行以下命令：
   ```bash
   pip download -r requirements.txt -d ./packages --platform manylinux_2_17_x86_64 --only-binary=:all: --python-version 3.8
   ```
   *注意：如果您的 NAS Python 版本是 3.9，请将 `--python-version 3.8` 改为 `3.9`。可以通过在 NAS 上 SSH 输入 `python3 --version` 查看。群晖 DSM 7.x 通常内置 Python 3.8。*

### 第二步：拷贝文件到 NAS
1. 将整个 `qiangongshi-New` 文件夹（包含 `packages` 文件夹和所有代码）通过 File Station 拷贝到 NAS 的某个目录，例如 `/volume1/homes/admin/qiangongshi`。

### 第三步：在 NAS 上安装并运行
1. **开启 SSH**：在群晖控制面板 -> 终端机和 SNMP -> 勾选“启动 SSH 功能”。
2. 使用 SSH 工具（如 PuTTY 或 CMD `ssh admin@NAS_IP`）登录 NAS。
3. 进入项目目录：
   ```bash
   cd /volume1/homes/admin/qiangongshi
   ```
4. 安装依赖（使用离线包）：
   ```bash
   python3 -m pip install --user --no-index --find-links=./packages -r requirements.txt
   ```
   *如果提示 `pip` 不存在，可能需要先安装 pip：`python3 -m ensurepip`*
5. 运行程序：
   ```bash
   python3 app.py
   ```
6. 如果需要后台运行（关闭 SSH 后不停止），使用：
   ```bash
   nohup python3 app.py > output.log 2>&1 &
   ```

---

## 建议

**强烈建议使用方案一（Docker）**。
原因：
1. 避免了 Python 版本不一致的问题。
2. 避免了缺少系统级依赖（如 pandas 需要的某些 C++ 库）的问题。
3. 管理方便，随开随用。

为了配合 Docker 方案的数据持久化，建议对代码做微小调整，将数据库文件路径改为可配置或固定在 `/app/data` 目录下，方便挂载。
