#!/usr/bin/env bash
# ============================================================
# EC2 一键部署脚本
# 用法: bash deploy.sh <EC2_HOST> [SSH_KEY_PATH]
# 例如: bash deploy.sh ubuntu@3.112.45.67 ~/.ssh/my-key.pem
# ============================================================
set -euo pipefail

HOST="${1:?用法: bash deploy.sh <user@host> [ssh_key_path]}"
KEY="${2:-}"
REMOTE_DIR="/home/ubuntu/arbitrage-bot"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
if [[ -n "$KEY" ]]; then
    SSH_OPTS="$SSH_OPTS -i $KEY"
fi

echo "=========================================="
echo " 部署到 $HOST"
echo "=========================================="

# ── 1. 创建远程目录 ──
echo "[1/5] 创建远程目录..."
ssh $SSH_OPTS "$HOST" "mkdir -p $REMOTE_DIR"

# ── 2. 上传文件 ──
echo "[2/5] 上传代码文件..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
scp $SSH_OPTS \
    "$SCRIPT_DIR/arbitrage_bot.py" \
    "$SCRIPT_DIR/binance_adapter.py" \
    "$SCRIPT_DIR/ws_manager.py" \
    "$SCRIPT_DIR/config.py" \
    "$SCRIPT_DIR/run.py" \
    "$SCRIPT_DIR/control_server.py" \
    "$SCRIPT_DIR/fill_handler.py" \
    "$SCRIPT_DIR/trade_logger.py" \
    "$SCRIPT_DIR/feishu_notifier.py" \
    "$SCRIPT_DIR/config.yaml" \
    "$SCRIPT_DIR/requirements.txt" \
    "$HOST:$REMOTE_DIR/"

# .env 单独传（如果存在）
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    echo "  上传 .env ..."
    scp $SSH_OPTS "$SCRIPT_DIR/.env" "$HOST:$REMOTE_DIR/"
else
    echo "  ⚠️  未找到 .env，请在 EC2 上手动配置"
fi

# ── 3. 安装依赖 ──
echo "[3/5] 安装 Python 依赖..."
ssh $SSH_OPTS "$HOST" << 'REMOTE_SCRIPT'
set -e
cd /home/ubuntu/arbitrage-bot

# 确保有 Python 3.10+
if ! command -v python3 &>/dev/null; then
    echo "安装 Python3..."
    sudo apt-get update -qq && sudo apt-get install -y -qq python3 python3-pip python3-venv
fi

# 创建虚拟环境
if [[ ! -d venv ]]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "依赖安装完成: $(pip list 2>/dev/null | grep -E 'binance|websockets|pyyaml' | tr '\n' ', ')"
REMOTE_SCRIPT

# ── 4. 创建 systemd 服务 ──
echo "[4/5] 配置 systemd 服务..."
ssh $SSH_OPTS "$HOST" << 'REMOTE_SCRIPT'
cat << 'SERVICE' | sudo tee /etc/systemd/system/arb-bot.service > /dev/null
[Unit]
Description=Spot-Futures Arbitrage Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/arbitrage-bot
ExecStart=/home/ubuntu/arbitrage-bot/venv/bin/python run.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

# 安全限制
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/home/ubuntu/arbitrage-bot

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl daemon-reload
sudo systemctl enable arb-bot
echo "systemd 服务已配置"
REMOTE_SCRIPT

# ── 5. 启动 ──
echo "[5/5] 启动机器人..."
ssh $SSH_OPTS "$HOST" << 'REMOTE_SCRIPT'
cd /home/ubuntu/arbitrage-bot
sudo systemctl restart arb-bot
sleep 2
if sudo systemctl is-active --quiet arb-bot; then
    echo "✅ 机器人已启动"
else
    echo "❌ 启动失败，查看日志:"
    sudo journalctl -u arb-bot --no-pager -n 20
fi
REMOTE_SCRIPT

echo ""
echo "=========================================="
echo " 部署完成!"
echo "=========================================="
echo ""
echo "常用命令:"
echo "  查看状态:  ssh $SSH_OPTS $HOST 'sudo systemctl status arb-bot'"
echo "  查看日志:  ssh $SSH_OPTS $HOST 'sudo journalctl -u arb-bot -f'"
echo "  查看文件日志: ssh $SSH_OPTS $HOST 'tail -f $REMOTE_DIR/arbitrage.log'"
echo "  停止:      ssh $SSH_OPTS $HOST 'sudo systemctl stop arb-bot'"
echo "  重启:      ssh $SSH_OPTS $HOST 'sudo systemctl restart arb-bot'"
echo "  修改配置:  ssh $SSH_OPTS $HOST 'nano $REMOTE_DIR/.env'"
echo ""
