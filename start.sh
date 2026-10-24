#!/usr/bin/env bash
# 闲鱼自动发货 —— 唯一入口。
#
#   ./start.sh           后台启动（首次自动建 .venv 并装依赖）
#   ./start.sh stop      停止
#   ./start.sh restart   重启
#   ./start.sh status    看状态
#   ./start.sh logs -f   跟日志
#   ./start.sh fg        前台运行，日志打在终端（调试用）
#
# 不给子命令时默认 start。真正干活的是 ctl.py —— 端口/pid/信号这些细节在 Python 里
# 说得清也测得动；这里只负责准备环境、把解释器找对。

set -euo pipefail

# 不管从哪调用，都切到脚本所在目录，否则 .env / data/ 的相对路径全错。
# 必须解析符号链接：把 start.sh 软链到 ~/bin 之类的目录时，BASH_SOURCE 指向的是
# 那个链接本身，dirname 会得到链接所在目录 —— 曾经因此在错误的地方建出 .venv。
resolve_dir() {
  local src="$1" dir
  while [ -L "$src" ]; do
    dir="$(cd -P "$(dirname "$src")" && pwd)"
    src="$(readlink "$src")"
    # 相对的链接目标是相对链接所在目录，不是相对当前目录
    case "$src" in
      /*) ;;
      *) src="$dir/$src" ;;
    esac
  done
  cd -P "$(dirname "$src")" && pwd
}
cd "$(resolve_dir "${BASH_SOURCE[0]}")"

VENV_DIR=".venv"
VENV_PY="$VENV_DIR/bin/python"
REQ="requirements.txt"
# 依赖标记：比 requirements.txt 新就说明装过了，不用再等 pip
STAMP="$VENV_DIR/.deps-installed"

# 只在真终端上色：重定向到文件 / CI 里不要塞一堆转义符
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_OK="$(printf '\033[32m')"
  C_ERR="$(printf '\033[31m')"
  C_DIM="$(printf '\033[2m')"
  C_OFF="$(printf '\033[0m')"
else
  C_OK=""; C_ERR=""; C_DIM=""; C_OFF=""
fi

info() { printf "%s\n" "$*"; }
ok()   { printf "%s✓%s %s\n" "$C_OK" "$C_OFF" "$*"; }
err()  { printf "%s✗%s %s\n" "$C_ERR" "$C_OFF" "$*" >&2; }
die()  { err "$*"; exit 1; }

usage() {
  # 把文件头的注释当帮助输出，省得两份说明走样
  sed -n "2,9p" "$0" | sed "s/^# \{0,1\}//"
}

find_python() {
  # 只要 3.10+：代码里用了 `str | None`，3.9 会在 import 期就崩，
  # 而报错信息完全看不出是版本问题，所以在这里提前拦住。
  local cand
  for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1 \
       && "$cand" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" 2>/dev/null; then
      printf "%s" "$cand"
      return 0
    fi
  done
  return 1
}

# 装依赖：优先用 pip；这个 venv 里没有 pip 时依次退到 uv、get-pip.py。
# 为什么不能直接假设有 pip：用 `uv venv` 或 `python -m venv --without-pip` 建的
# 虚拟环境根本没有 pip，一句 `python -m pip` 就报 "No module named pip"，
# 而这个报错特别容易让人以为是网络或权限问题。
install_requirements() {
  local py="$1"

  if "$py" -m pip --version >/dev/null 2>&1; then
    "$py" -m pip install --quiet --upgrade pip >/dev/null 2>&1 || true
    "$py" -m pip install --quiet -r "$REQ"
    return $?
  fi

  if command -v uv >/dev/null 2>&1; then
    info "${C_DIM}（这个虚拟环境没有 pip，改用 uv 安装）${C_OFF}"
    # uv 默认把缓存写 ~/.cache/uv；只读 HOME / 容器里会失败，退到项目内的 .uvcache
    if ! uv pip install --quiet --python "$py" -r "$REQ" 2>/dev/null; then
      UV_CACHE_DIR="$PWD/.uvcache" uv pip install --quiet --python "$py" -r "$REQ" || return 1
    fi
    return 0
  fi

  info "${C_DIM}（没有 pip 也没有 uv，尝试引导 pip）${C_OFF}"
  local getpip=".venv/get-pip.py"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL https://bootstrap.pypa.io/get-pip.py -o "$getpip" 2>/dev/null || true
  elif command -v wget >/dev/null 2>&1; then
    wget -q https://bootstrap.pypa.io/get-pip.py -O "$getpip" 2>/dev/null || true
  fi
  if [ -s "$getpip" ] && "$py" "$getpip" >/dev/null 2>&1; then
    rm -f "$getpip"
    "$py" -m pip install --quiet -r "$REQ"
    return $?
  fi
  rm -f "$getpip"
  return 1
}

ensure_env() {
  if [ ! -x "$VENV_PY" ]; then
    local sys_py
    sys_py="$(find_python)" || die "找不到 Python 3.10+。Debian/Ubuntu: sudo apt install python3 python3-venv"
    info "${C_DIM}首次运行：用 $sys_py 创建虚拟环境 …${C_OFF}"
    "$sys_py" -m venv "$VENV_DIR" \
      || die "创建虚拟环境失败。Debian/Ubuntu 可能需要: sudo apt install python3-venv"
    ok "虚拟环境已创建"
  fi

  if [ -f "$STAMP" ] && [ "$STAMP" -nt "$REQ" ]; then
    return 0
  fi

  info "${C_DIM}安装依赖（首次会慢一点）…${C_OFF}"
  install_requirements "$VENV_PY" \
    || die "依赖安装失败。手动重试: uv pip install -r $REQ （或 $VENV_PY -m pip install -r $REQ）"
  touch "$STAMP"
  ok "依赖已就绪"
}

CMD="${1:-start}"
[ $# -gt 0 ] && shift

# help 在准备环境之前就返回：没必要为了看用法装一遍依赖
# （必须放在 ensure_env 前面，所以不能用下面那张 dispatch 表统一处理）
if [ "$CMD" = "-h" ] || [ "$CMD" = "--help" ] || [ "$CMD" = "help" ]; then
  usage; exit 0
fi

# status / stop 只读 .env 和 pid 文件，不装依赖也得能用 ——
# 服务起不来、依赖装坏的时候，你依然得能把进程停掉。
case "$CMD" in
  status|st|stop|down) needs_env=skip ;;
  *) needs_env=full ;;
esac

if [ "$needs_env" = "skip" ] && [ -x "$VENV_PY" ]; then
  : # 有解释器就够了，直接往下走
else
  ensure_env
fi

case "$CMD" in
  start|up)
    exec "$VENV_PY" ctl.py start "$@"
    ;;
  stop|down)
    exec "$VENV_PY" ctl.py stop "$@"
    ;;
  restart)
    exec "$VENV_PY" ctl.py restart "$@"
    ;;
  status|st)
    exec "$VENV_PY" ctl.py status "$@"
    ;;
  logs|log)
    exec "$VENV_PY" ctl.py logs "$@"
    ;;
  fg|foreground)
    # 前台跑 = 原来的 main.py，直接 exec 替换掉当前 shell（Ctrl-C 就能停）
    exec "$VENV_PY" main.py "$@"
    ;;
  *)
    err "不认识的命令: $CMD"
    echo
    usage
    exit 2
    ;;
esac
