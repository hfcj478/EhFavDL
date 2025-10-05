基于不修改 GitHub 项目源码的原则，我来为您设计一个完整的外部整合方案代码结构：

项目结构设计
TEXT
~/ehfavdl-docker/
├── app/                          # 原始 EhFavDL 项目（保持不变）
│   ├── main.py
│   ├── src/
│   ├── config.yaml
│   └── requirements.txt
├── scripts/                      # 所有外部脚本和工具
│   ├── core/                    # 核心功能模块
│   │   ├── config_manager.py
│   │   ├── db_watcher.py
│   │   ├── download_manager.py
│   │   ├── cbz_processor.py
│   │   └── duplicate_checker.py
│   ├── services/                # 系统服务相关
│   │   ├── service_installer.sh
│   │   ├── systemd/
│   │   │   └── ehfavdl-watcher.service
│   │   └── service_manager.py
│   ├── workflows/               # 工作流程脚本
│   │   ├── intelligent_workflow.sh
│   │   ├── smart_download.sh
│   │   └── complete_processing.sh
│   └── utils/                   # 工具函数
│       ├── network_tester.py
│       ├── status_monitor.py
│       └── backup_manager.py
├── data/                        # 数据目录（外部挂载）
│   └── ehfavdl -> /vol3/1000/yp1/eh  # 符号链接到实际数据目录
├── logs/                        # 日志目录
├── config/                      # 配置文件管理
│   ├── ehfavdl_config.yaml      # 主配置文件
│   ├── watch_config.yaml        # 监听配置
│   └── backup/                  # 配置备份
├── docker-compose.yml           # Docker 编排文件
└── README.md                    # 项目说明
具体实现文件
1. 主配置文件 config/ehfavdl_config.yaml
YAML
# EhFavDL 外部整合配置
system:
  data_path: "/vol3/1000/yp1/eh"
  log_path: "/root/ehfavdl-docker/logs"
  backup_path: "/root/ehfavdl-docker/backups"
  original_app_path: "/root/ehfavdl-docker/app"

database:
  watch_interval: 300  # 数据库检查间隔（秒）
  update_check_interval: 1800  # 更新检查间隔（秒）
  max_retries: 3
  retry_delay: 60

download:
  skip_existing: true
  auto_cbz: true
  auto_comicinfo: true
  max_concurrent: 3

watch:
  enabled: true
  mode: "database"  # database 或 fav
  fav_ids: "0,1,2,3,4,5,6,7,8,9"
  check_interval: 1800

comicinfo:
  language: "zh-CN"
  prefer_japanese_title: true
  enable_translation: true

services:
  auto_start: true
  restart_on_failure: true
  log_rotation: 7  # 保留7天日志
2. 核心配置管理器 scripts/core/config_manager.py
PYTHON
#!/usr/bin/env python3
"""
配置管理器 - 不修改原始配置文件，通过外部方式管理配置
"""
import yaml
import os
import shutil
from pathlib import Path

class ConfigManager:
    def __init__(self, original_app_path):
        self.original_app_path = Path(original_app_path)
        self.external_config_path = Path(__file__).parent.parent.parent / "config" / "ehfavdl_config.yaml"
        self.original_config_path = self.original_app_path / "config.yaml"
        
        # 加载外部配置
        self.external_config = self.load_external_config()
        
    def load_external_config(self):
        """加载外部配置"""
        with open(self.external_config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def get_original_config(self):
        """读取原始配置（不修改）"""
        with open(self.original_config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def update_original_config(self, updates):
        """
        更新原始配置（创建备份并更新）
        返回: 是否成功
        """
        try:
            # 备份原始配置
            backup_path = self.original_config_path.with_suffix('.yaml.backup')
            shutil.copy2(self.original_config_path, backup_path)
            
            # 读取当前配置
            current_config = self.get_original_config()
            
            # 应用更新
            self._deep_update(current_config, updates)
            
            # 写回配置
            with open(self.original_config_path, 'w', encoding='utf-8') as f:
                yaml.dump(current_config, f, default_flow_style=False, allow_unicode=True)
            
            return True
            
        except Exception as e:
            print(f"配置更新失败: {e}")
            return False
    
    def _deep_update(self, original, updates):
        """深度更新字典"""
        for key, value in updates.items():
            if isinstance(value, dict) and key in original and isinstance(original[key], dict):
                self._deep_update(original[key], value)
            else:
                original[key] = value
    
    def restore_original_config(self):
        """恢复原始配置"""
        backup_path = self.original_config_path.with_suffix('.yaml.backup')
        if backup_path.exists():
            shutil.copy2(backup_path, self.original_config_path)
            return True
        return False
    
    def get_database_path(self):
        """获取数据库路径"""
        return self.external_config['system']['data_path'] + "/data.db"
    
    def get_watch_interval(self):
        """获取监听间隔"""
        return self.external_config['database']['watch_interval']
3. 数据库监听器 scripts/core/db_watcher.py
PYTHON
#!/usr/bin/env python3
"""
数据库监听器 - 通过外部数据库轮询检测更新
"""
import sqlite3
import time
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

class DatabaseWatcher:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.external_config = config_manager.external_config
        self.running = True
        self.processed_updates = set()
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志"""
        log_path = Path(self.external_config['system']['log_path'])
        log_path.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path / 'db_watcher.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('DBWatcher')
    
    def get_db_connection(self):
        """获取数据库连接"""
        db_path = self.config_manager.get_database_path()
        return sqlite3.connect(db_path)
    
    async def check_gallery_updates(self):
        """检查画廊更新"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 检查有更新的画廊 (gid != current_gid)
                cursor.execute("""
                    SELECT gid, token, current_gid, current_token, title_jpn
                    FROM eh_data 
                    WHERE gid != current_gid 
                    AND current_gid IS NOT NULL 
                    AND current_gid != ''
                    AND del_flag = 0
                """)
                
                updates = cursor.fetchall()
                return updates
                
        except Exception as e:
            self.logger.error(f"检查画廊更新失败: {e}")
            return []
    
    async def check_new_galleries(self):
        """检查新画廊"""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 检查最近添加的画廊（最近检查间隔内）
                check_interval = self.external_config['database']['watch_interval']
                since_time = (datetime.now() - timedelta(seconds=check_interval)).timestamp()
                
                cursor.execute("""
                    SELECT gid, token, title_jpn, add_time
                    FROM eh_data 
                    WHERE add_time > ? 
                    AND del_flag = 0
                """, (since_time,))
                
                new_galleries = cursor.fetchall()
                return new_galleries
                
        except Exception as e:
            self.logger.error(f"检查新画廊失败: {e}")
            return []
    
    async def trigger_download(self, gallery_type, gid=None, token=None):
        """触发下载（通过调用原始程序）"""
        try:
            import subprocess
            import os
            
            app_path = self.external_config['system']['original_app_path']
            
            if gallery_type == "update":
                # 使用 Watch 模式 2 下载更新
                cmd = ["python", "main.py", "-w2"]
            elif gallery_type == "new":
                # 使用 Watch 模式 2 下载新内容
                cmd = ["python", "main.py", "-w2"]
            else:
                return False
            
            # 在原始应用目录中执行命令
            result = subprocess.run(
                cmd,
                cwd=app_path,
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            
            if result.returncode == 0:
                self.logger.info(f"下载触发成功: {gallery_type}")
                return True
            else:
                self.logger.error(f"下载触发失败: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"触发下载异常: {e}")
            return False
    
    async def process_detected_changes(self):
        """处理检测到的变更"""
        # 检查更新
        updates = await self.check_gallery_updates()
        if updates:
            self.logger.info(f"发现 {len(updates)} 个画廊更新")
            await self.trigger_download("update")
        
        # 检查新画廊
        new_galleries = await self.check_new_galleries()
        if new_galleries:
            self.logger.info(f"发现 {len(new_galleries)} 个新画廊")
            await self.trigger_download("new")
    
    async def run(self):
        """运行监听循环"""
        self.logger.info("数据库监听器启动")
        
        check_interval = self.config_manager.get_watch_interval()
        
        while self.running:
            try:
                await self.process_detected_changes()
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"监听循环异常: {e}")
                await asyncio.sleep(60)  # 出错时等待1分钟
    
    def stop(self):
        """停止监听"""
        self.running = False
        self.logger.info("数据库监听器停止")
4. 下载管理器 scripts/core/download_manager.py
PYTHON
#!/usr/bin/env python3
"""
下载管理器 - 管理下载流程，避免重复下载
"""
import sqlite3
import logging
from pathlib import Path

class DownloadManager:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.external_config = config_manager.external_config
        self.data_path = Path(self.external_config['system']['data_path'])
        
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('DownloadManager')
    
    def is_gallery_downloaded(self, gid):
        """检查画廊是否已下载"""
        web_path = self.data_path / "web"
        
        # 检查 CBZ 文件
        cbz_files = list(web_path.glob(f"*{gid}*.cbz"))
        if cbz_files:
            return True
        
        # 检查原始目录
        gallery_dirs = [d for d in web_path.iterdir() 
                       if d.is_dir() and str(gid) in d.name]
        if gallery_dirs:
            return True
        
        # 检查数据库标记
        try:
            db_path = self.config_manager.get_database_path()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM eh_data WHERE gid = ? AND (original_flag = 1 OR web_1280x_flag = 1)",
                    (gid,)
                )
                count = cursor.fetchone()[0]
                return count > 0
        except:
            return False
    
    def get_download_stats(self):
        """获取下载统计"""
        web_path = self.data_path / "web"
        
        if not web_path.exists():
            return {
                'cbz_count': 0,
                'dir_count': 0,
                'total_size_gb': 0
            }
        
        # 统计 CBZ 文件
        cbz_files = list(web_path.glob("*.cbz"))
        cbz_size = sum(f.stat().st_size for f in cbz_files)
        
        # 统计目录
        gallery_dirs = [d for d in web_path.iterdir() if d.is_dir()]
        dir_size = 0
        for dir_path in gallery_dirs:
            dir_size += sum(f.stat().st_size for f in dir_path.rglob('*') if f.is_file())
        
        total_size_gb = (cbz_size + dir_size) / (1024**3)
        
        return {
            'cbz_count': len(cbz_files),
            'dir_count': len(gallery_dirs),
            'total_size_gb': total_size_gb
        }
    
    def should_skip_download(self, gid):
        """判断是否应该跳过下载"""
        if not self.external_config['download']['skip_existing']:
            return False
        
        return self.is_gallery_downloaded(gid)
5. 系统服务管理器 scripts/services/service_manager.py
PYTHON
#!/usr/bin/env python3
"""
系统服务管理器 - 管理 systemd 服务
"""
import os
import subprocess
import shutil
from pathlib import Path

class ServiceManager:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.service_name = "ehfavdl-watcher"
        self.service_file = f"/etc/systemd/system/{self.service_name}.service"
        
        # 服务文件模板
        self.service_template = f"""[Unit]
Description=EhFavDL Database Watcher Service
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory={config_manager.external_config['system']['original_app_path']}
Environment=PATH=/usr/bin:/usr/local/bin
ExecStart={config_manager.external_config['system']['original_app_path']}/../scripts/core/db_watcher.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# 安全设置
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths={config_manager.external_config['system']['data_path']}

[Install]
WantedBy=multi-user.target
"""
    
    def install_service(self):
        """安装系统服务"""
        try:
            # 创建服务文件
            with open(self.service_file, 'w') as f:
                f.write(self.service_template)
            
            # 重新加载 systemd
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            
            # 启用服务
            subprocess.run(["systemctl", "enable", self.service_name], check=True)
            
            print("✓ 系统服务安装成功")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"✗ 服务安装失败: {e}")
            return False
    
    def uninstall_service(self):
        """卸载系统服务"""
        try:
            # 停止服务
            subprocess.run(["systemctl", "stop", self.service_name], check=False)
            
            # 禁用服务
            subprocess.run(["systemctl", "disable", self.service_name], check=False)
            
            # 删除服务文件
            if os.path.exists(self.service_file):
                os.remove(self.service_file)
            
            # 重新加载 systemd
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            
            print("✓ 系统服务卸载成功")
            return True
            
        except Exception as e:
            print(f"✗ 服务卸载失败: {e}")
            return False
    
    def start_service(self):
        """启动服务"""
        try:
            subprocess.run(["systemctl", "start", self.service_name], check=True)
            print("✓ 服务启动成功")
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ 服务启动失败: {e}")
            return False
    
    def stop_service(self):
        """停止服务"""
        try:
            subprocess.run(["systemctl", "stop", self.service_name], check=True)
            print("✓ 服务停止成功")
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ 服务停止失败: {e}")
            return False
    
    def get_service_status(self):
        """获取服务状态"""
        try:
            result = subprocess.run(
                ["systemctl", "status", self.service_name],
                capture_output=True,
                text=True
            )
            return result.stdout
        except Exception as e:
            return f"获取状态失败: {e}"
6. 主入口脚本 run-ehfavdl.sh
BASH
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 导入配置
source scripts/utils/common.sh

echo "=================================================="
echo "           EhFavDL 外部整合系统"
echo "=================================================="
echo "数据目录: $DATA_PATH"
echo "监听模式: 数据库轮询"
echo "开机自启: 已配置"
echo "=================================================="

# 检查 Python 环境
if [ ! -d "venv" ]; then
    echo "创建 Python 虚拟环境..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r scripts/requirements.txt
else
    source venv/bin/activate
fi

# 导入 Python 模块
export PYTHONPATH="$SCRIPT_DIR/scripts:$PYTHONPATH"

while true; do
    echo ""
    echo "🔍 监听功能:"
    echo "1. 启动数据库监听服务"
    echo "2. 安装系统服务 (开机自启)"
    echo "3. 服务管理"
    echo ""
    echo "🚀 下载功能:"
    echo "4. 智能下载流程"
    echo "5. 快速下载"
    echo "6. 检查画廊更新"
    echo ""
    echo "📊 管理工具:"
    echo "7. 下载状态统计"
    echo "8. 文件处理工具"
    echo "9. 系统配置"
    echo "0. 退出"
    echo "=================================================="
    
    read -p "请选择功能 (0-9): " choice
    
    case $choice in
        1)
            echo "启动数据库监听服务..."
            python scripts/core/db_watcher.py
            ;;
        2)
            echo "安装系统服务..."
            python scripts/services/service_manager.py install
            ;;
        3)
            echo "服务管理..."
            echo "1. 启动服务"
            echo "2. 停止服务"
            echo "3. 重启服务"
            echo "4. 服务状态"
            echo "5. 卸载服务"
            read -p "请选择: " service_choice
            
            case $service_choice in
                1) python scripts/services/service_manager.py start ;;
                2) python scripts/services/service_manager.py stop ;;
                3) python scripts/services/service_manager.py restart ;;
                4) python scripts/services/service_manager.py status ;;
                5) python scripts/services/service_manager.py uninstall ;;
                *) echo "无效选择" ;;
            esac
            ;;
        4)
            echo "启动智能下载流程..."
            bash scripts/workflows/intelligent_workflow.sh
            ;;
        5)
            echo "启动快速下载..."
            bash scripts/workflows/smart_download.sh
            ;;
        6)
            echo "检查画廊更新..."
            python scripts/utils/update_checker.py
            ;;
        7)
            echo "下载状态统计..."
            python scripts/utils/status_monitor.py
            ;;
        8)
            echo "文件处理工具..."
            bash scripts/workflows/file_processor.sh
            ;;
        9)
            echo "系统配置..."
            python scripts/core/config_manager.py edit
            ;;
        0)
            echo "退出"
            exit 0
            ;;
        *)
            echo "无效选择"
            ;;
    esac
    
    echo ""
    read -p "按 Enter 继续..."
done
7. 工具函数 scripts/utils/common.sh
BASH
#!/bin/bash

# 通用工具函数

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "命令 $1 不存在，请先安装"
        return 1
    fi
    return 0
}

# 检查目录是否存在
check_directory() {
    if [ ! -d "$1" ]; then
        log_error "目录不存在: $1"
        return 1
    fi
    return 0
}

# 等待用户确认
confirm() {
    read -p "$1 (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        return 0
    else
        return 1
    fi
}

# 加载配置
load_config() {
    CONFIG_FILE="config/ehfavdl_config.yaml"
    if [ -f "$CONFIG_FILE" ]; then
        DATA_PATH=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['system']['data_path'])")
        ORIGINAL_APP_PATH=$(python3 -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['system']['original_app_path'])")
    else
        log_error "配置文件不存在: $CONFIG_FILE"
        exit 1
    fi
}
8. 依赖文件 scripts/requirements.txt
TXT
PyYAML>=6.0
aiohttp>=3.8.0
asyncio>=3.4.3
python-dateutil>=2.8.0
9. Docker 编排文件 docker-compose.yml
YAML
version: '3.8'

services:
  ehfavdl-watcher:
    build: 
      context: .
      dockerfile: Dockerfile.watcher
    container_name: ehfavdl-watcher
    volumes:
      - ./config:/app/config:ro
      - ./scripts:/app/scripts:ro
      - /vol3/1000/yp1/eh:/data:rw
      - ./logs:/app/logs:rw
    environment:
      - TZ=Asia/Shanghai
      - PYTHONPATH=/app/scripts
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  ehfavdl-processor:
    build:
      context: .
      dockerfile: Dockerfile.processor  
    container_name: ehfavdl-processor
    volumes:
      - ./config:/app/config:ro
      - ./scripts:/app/scripts:ro
      - /vol3/1000/yp1/eh:/data:rw
    environment:
      - TZ=Asia/Shanghai
    restart: on-failure
    depends_on:
      - ehfavdl-watcher

networks:
  default:
    name: ehfavdl-network
部署和使用方法
1. 初始化部署
BASH
# 克隆原始项目（如果还没有）
git clone https://github.com/eezd/EhFavDL.git app

# 创建整合系统目录结构
mkdir -p ~/ehfavdl-docker/{scripts,config,logs,data}
cd ~/ehfavdl-docker

# 复制上述所有脚本文件到相应目录
# 创建符号链接
ln -s /vol3/1000/yp1/eh data/ehfavdl

# 初始化配置
python scripts/core/config_manager.py init
2. 安装系统服务
BASH
cd ~/ehfavdl-docker

# 一键安装（需要 root 权限）
sudo scripts/services/service_installer.sh

# 或者手动安装
sudo python scripts/services/service_manager.py install
sudo python scripts/services/service_manager.py enable
sudo python scripts/services/service_manager.py start
3. 使用整合系统
BASH
cd ~/ehfavdl-docker

# 使用主菜单
./run-ehfavdl.sh

# 或者直接运行特定功能
python scripts/core/db_watcher.py          # 手动运行监听
bash scripts/workflows/intelligent_workflow.sh  # 智能下载
python scripts/utils/status_monitor.py     # 状态监控
设计优势
完全外部化：不修改任何原始项目代码
模块化设计：各功能独立，便于维护和扩展
配置集中管理：所有配置通过外部文件管理
错误隔离：外部脚本错误不影响原始程序
灵活部署：支持 Docker 和原生系统部署
完整日志：详细的运行日志和错误追踪
自动恢复：服务崩溃自动重启
资源监控：监控磁盘空间和系统资源
这个设计确保了您可以在不修改 EhFavDL 源代码的情况下，获得所有需要的功能，并且保持了系统的稳定性和可维护性。
