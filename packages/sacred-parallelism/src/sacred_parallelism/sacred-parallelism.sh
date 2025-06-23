#!/bin/bash

set -e

####################################
# Logging functions with timestamps
####################################
log() {
  local level="$1"
  local message="$2"
  local timestamp
  timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$timestamp] [$level] $message"
}

log_info() {
  log "INFO" "$1"
}

log_error() {
  log "ERROR" "$1" >&2
}

####################################
# Check and parse script arguments
####################################
if [[ "$1" == "help" || "$1" == "--help" ]]; then
  echo "Usage:"
  echo "  $0 run <experiment_folder> [--clean-logs] [-n|--dry-run] [-m|--monitor <interval>]"
  echo ""
  echo "Options:"
  echo "  -n, --dry-run       Print commands instead of executing"
  echo "  -m, --monitor INT   Enable system/proc monitoring every INT seconds"
  echo "  --clean-logs        Remove previous logs before this run"
  exit 0
fi
if [[ "$1" != "run" ]]; then
  log_error "Usage: $0 run <experiment_folder> [--clean-logs] [-n|--dry-run] [-m|--monitor <interval>]"
  log_error "Type '$0 help' for more information."
  exit 1
fi
shift

FOLDER="$1"
DRY_RUN=""
IS_DRY_RUN=false
MONITOR_INTERVAL=""
ENABLE_PROC_MONITORING=false

# Parse optional monitor argument
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--dry-run)
      DRY_RUN="--dry-run"
      IS_DRY_RUN=true
      shift
      ;;
    -m|--monitor)
      MONITOR_INTERVAL="$2"
      ENABLE_PROC_MONITORING=true
      if [[ -z "$MONITOR_INTERVAL" ]]; then
        log_error "Monitor interval not specified after --monitor"
        exit 1
      fi
      if ! [[ "$MONITOR_INTERVAL" =~ ^[0-9]+$ ]]; then
        log_error "Invalid monitor interval: $MONITOR_INTERVAL. Must be a positive integer."
        exit 1
      fi
      shift 2
      ;;
    --clean-logs)
      CLEAN_LOGS=true
      shift
      ;;
    *)
      FOLDER="$1"
      shift
      ;;
  esac
done
# Resolve full path
FULL_FOLDER_PATH=$(realpath "$FOLDER")
EXPERIMENT_FILE="$FULL_FOLDER_PATH/experiment.py"
CONFIG_FILE="$FULL_FOLDER_PATH/config.yaml"

# Prepare logs directory with timestamp
BASE_LOGS_DIR="$FULL_FOLDER_PATH/logs"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RUN_LOG_DIR="$BASE_LOGS_DIR/$TIMESTAMP"
LATEST_LINK="$BASE_LOGS_DIR/latest"
JOBS_LOG_DIR="$RUN_LOG_DIR/jobs"
STATS_CSV="$RUN_LOG_DIR/stats.csv"
mkdir -p "$JOBS_LOG_DIR"
# Clean previous log directories if requested
if [[ "$CLEAN_LOGS" == true ]]; then
  log_info "Cleaning old logs in $BASE_LOGS_DIR"
  find "$BASE_LOGS_DIR" -mindepth 1 -maxdepth 1 -type d ! -name "$(basename "$RUN_LOG_DIR")" -exec rm -rf {} +
fi

# Redirect all output to sacred-parallelism.log
SCRIPT_LOG="$RUN_LOG_DIR/sacred-parallelism.log"
exec > >(tee -a "$SCRIPT_LOG") 2>&1

####################################
# Machine characteristics logging
####################################
log_info "Machine info:"
log_info "  Hostname: $(hostname)"
log_info "  OS: $(uname -s) $(uname -r)"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  log_info "  CPU: $(grep -m1 'model name' /proc/cpuinfo | cut -d ':' -f2 | xargs)"
  log_info "  Cores: $(nproc)"
  log_info "  Total Memory: $(free -h | awk '/Mem:/ { print $2 }')"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  log_info "  CPU: $(sysctl -n machdep.cpu.brand_string)"
  log_info "  Cores: $(sysctl -n hw.ncpu)"
  mem_bytes=$(sysctl -n hw.memsize)
  mem_gb=$(awk "BEGIN {printf \"%.1fGB\", $mem_bytes / (1024^3)}")
  log_info "  Total Memory: $mem_gb"
else
  log_info "  CPU: Unknown"
  log_info "  Cores: Unknown"
  log_info "  Total Memory: Unknown"
fi

log_info "Using directory: $FULL_FOLDER_PATH"
log_info "Logging run in: $RUN_LOG_DIR"
[[ "$IS_DRY_RUN" == true ]] && log_info "Dry-run mode enabled (delay forced to 0)"


start_monitoring() {
  log_info "Monitoring system stats every $MONITOR_INTERVAL seconds into $STATS_CSV"
  {
    echo "timestamp,cpu_usage_percent,cpu_idle_percent,load_avg_1min,mem_used_MB,mem_total_MB,swap_used_MB,disk_used_percent"
    while true; do
      timestamp=$(date +"%Y-%m-%d %H:%M:%S")
      if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        cpu_idle=$(top -bn1 | grep "Cpu(s)" | awk '{print $8}')
        cpu_used=$(awk "BEGIN {print 100 - $cpu_idle}")
        load=$(cut -d ' ' -f1 < /proc/loadavg)
        mem_used=$(free -m | awk '/Mem:/ {print $3}')
        mem_total=$(free -m | awk '/Mem:/ {print $2}')
        swap_used=$(free -m | awk '/Swap:/ {print $3}')
        disk_used=$(df / | awk 'END{print $(NF-1)}' | tr -d '%')
      elif [[ "$OSTYPE" == "darwin"* ]]; then
        cpu_used=$(ps -A -o %cpu | awk '{s+=$1} END {print s}')
        cpu_idle=$(top -l 1 | grep 'CPU usage' | awk '{print $7}' | tr -d '%idle,')
        load=$(sysctl -n vm.loadavg | awk '{print $2}' | tr -d '{}')
        mem_total=$(sysctl -n hw.memsize)
        mem_total=$(awk "BEGIN {print int($mem_total / 1024 / 1024)}")
        mem_used=$(vm_stat | grep 'Pages active' | awk '{print $3}' | tr -d '.' | awk '{print int($1 * 4096 / 1024 / 1024)}')
        swap_used=$(sysctl -n vm.swapusage | awk '{print $7}' | tr -d 'M')
        disk_used=$(df / | awk 'END{print $(NF-1)}' | tr -d '%')
      else
        cpu_used=0; cpu_idle=0; load=0; mem_used=0; mem_total=0; swap_used=0; disk_used=0
      fi
      echo "$timestamp,$cpu_used,$cpu_idle,$load,$mem_used,$mem_total,$swap_used,$disk_used"
      sleep "$MONITOR_INTERVAL"
    done
  } >> "$STATS_CSV" &
  MONITOR_PID=$!
}

stop_monitoring() {
  if [[ -n "$MONITOR_PID" ]]; then
    log_info "Stopping system monitor (PID $MONITOR_PID)"
    kill "$MONITOR_PID" 2>/dev/null || true
  fi
}

trap stop_monitoring EXIT INT TERM

####################################
# Validate required files
####################################
if [[ ! -f "$EXPERIMENT_FILE" ]]; then
  log_error "Missing experiment.py in $FULL_FOLDER_PATH"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  log_error "Missing config.yaml in $FULL_FOLDER_PATH"
  exit 1
fi

log_info "Found experiment.py and config.yaml"

####################################
# Generate parameter combinations
####################################
log_info "Generating parameter combinations..."

JOBS_FILE="$RUN_LOG_DIR/jobs.txt"

PARAM_COMBINATIONS=$(python3 - <<EOF
import yaml
import itertools

with open("$CONFIG_FILE") as f:
    cfg = yaml.safe_load(f)

params = cfg.get("params", {})
keys = list(params.keys())
values = list(params.values())

for combo in itertools.product(*values):
    args = " ".join(f"{k}={v}" for k, v in zip(keys, combo))
    print(f"python3 experiment.py with {args}")
EOF
)

echo "$PARAM_COMBINATIONS" > "$JOBS_FILE"
log_info "Written job list to $JOBS_FILE"

####################################
# Read parallel settings from config
####################################
log_info "Reading parallel configuration..."

read_yaml_value() {
  python3 -c "import yaml; print((yaml.safe_load(open('$CONFIG_FILE'))['parallel'].get('$1', '')))"
}

JOBS=$(read_yaml_value jobs)
LOAD=$(read_yaml_value load)
MEMFREE=$(read_yaml_value memfree)
MEMSUSPEND=$(read_yaml_value memsuspend)
DELAY=$(read_yaml_value delay)
[[ "$IS_DRY_RUN" == true ]] && DELAY=0
JOBLOG_REL=$(read_yaml_value joblog)
RESULTS_REL=$(read_yaml_value results)
FLAGS_RAW=$(read_yaml_value flags)

# Prepare full paths
JOBLOG=""
[[ -n "$JOBLOG_REL" ]] && JOBLOG="$RUN_LOG_DIR/$(basename "$JOBLOG_REL")"

RESULTS="$RUN_LOG_DIR"
[[ -n "$RESULTS_REL" ]] && RESULTS="$RUN_LOG_DIR/$(basename "$RESULTS_REL")"
mkdir -p "$RESULTS"

# Parse flags
FLAGS=""
if [[ -n "$FLAGS_RAW" ]]; then
  FLAGS=$(echo "$FLAGS_RAW" | tr -d "[],'" | xargs -n1 | sed 's/^/--/' | tr '\n' ' ')
fi

####################################
# Log the full parallel configuration
####################################
log_info "Parallel configuration:"
[[ -n "$JOBS" ]] && log_info "  jobs       = $JOBS"
[[ -n "$LOAD" ]] && log_info "  load        $LOAD"
[[ -n "$MEMFREE" ]] && log_info "  memfree    = $MEMFREE"
[[ -n "$MEMSUSPEND" ]] && log_info "  memsuspend = $MEMSUSPEND"
[[ -n "$DELAY" ]] && log_info "  delay      = $DELAY"
[[ -n "$JOBLOG" ]] && log_info "  joblog     = $JOBLOG"
[[ -n "$FLAGS" ]] && log_info "  flags      = $FLAGS"

####################################
# Create per-job logging folder wrappers
####################################
log_info "Creating per-job output folders..."

WRAPPED_JOBS_FILE="$RUN_LOG_DIR/jobs_wrapped.txt"
> "$WRAPPED_JOBS_FILE"

while IFS= read -r line; do
  safe_name=$(echo "$line" | sed 's/[^a-zA-Z0-9._=-]/_/g' | cut -c1-200)
  job_dir="$JOBS_LOG_DIR/$safe_name"
  mkdir -p "$job_dir"
  out_file="$job_dir/stdout.txt"
  err_file="$job_dir/stderr.txt"
  proc_stat_file="$job_dir/proc_stats.csv"
escaped_line=$(printf '%q' "$line")
if [[ "$ENABLE_PROC_MONITORING" == true ]]; then
  echo "timestamp,%cpu,%mem,elapsed" > "$proc_stat_file"
  wrapped_job=$(
    cat <<EOF
(
echo "timestamp,%cpu,%mem,elapsed";
while true; do
  ts=\$(date +%Y-%m-%d\ %H:%M:%S);
  ps -p $$ -o %cpu=,%mem=,etime= | awk -v ts="\$ts" '{print ts","\$1","\$2","\$3}' >> "$proc_stat_file";
  sleep 1;
done &
MON_PID=\$!;
$line > >(tee "$out_file") 2> >(tee "$err_file" >&2);
kill \$MON_PID 2>/dev/null
)
EOF
  )
else
  wrapped_job=$(
    cat <<EOF
(
$line > >(tee "$out_file") 2> >(tee "$err_file" >&2)
)
EOF
  )
fi
# collapse to single line
echo "$wrapped_job" | tr '\n' ' ' >> "$WRAPPED_JOBS_FILE"
echo >> "$WRAPPED_JOBS_FILE"  # newline to separate jobs
done < "$JOBS_FILE"

####################################
# Update 'latest' symlink
####################################
ln -sfn "$RUN_LOG_DIR" "$LATEST_LINK"
log_info "Updated latest symlink to $RUN_LOG_DIR"

####################################
# Build and log the full parallel command dynamically
####################################
PARALLEL_CMD="cd \"$FULL_FOLDER_PATH\" && parallel $DRY_RUN"
[[ -n "$JOBS" ]] && PARALLEL_CMD+=" --jobs \"$JOBS\""
[[ -n "$LOAD" ]] && PARALLEL_CMD+=" --load \"$LOAD\""
[[ -n "$MEMFREE" ]] && PARALLEL_CMD+=" --memfree \"$MEMFREE\""
[[ -n "$MEMSUSPEND" ]] && PARALLEL_CMD+=" --memsuspend \"$MEMSUSPEND\""
[[ -n "$DELAY" ]] && PARALLEL_CMD+=" --delay \"$DELAY\""
[[ -n "$JOBLOG" ]] && PARALLEL_CMD+=" --joblog \"$JOBLOG\""
[[ -n "$FLAGS" ]] && PARALLEL_CMD+=" $FLAGS"
PARALLEL_CMD+=" < \"$WRAPPED_JOBS_FILE\""

log_info "Full parallel command to be executed:"
log_info "$PARALLEL_CMD"

####################################
# Execute jobs
####################################
[[ -n "$MONITOR_INTERVAL" ]] && start_monitoring

log_info "Launching parallel execution..."
cd "$FULL_FOLDER_PATH"
eval "$PARALLEL_CMD"

log_info "Parallel execution finished."
log_info "All logs and job outputs saved in: $RUN_LOG_DIR"a