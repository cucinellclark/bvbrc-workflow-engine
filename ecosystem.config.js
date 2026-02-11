// Port configuration
const API_HOST = '140.221.78.67';
const API_PORT = 12007;
const METRICS_PORT = 9090;

// Base paths
const BASE_DIR = '/home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine';

module.exports = {
  apps: [
    {
      name: 'workflow-engine-api',
      script: './scripts/start_api.sh',
      interpreter: 'bash',
      cwd: BASE_DIR,
      env: {
        PYTHONPATH: BASE_DIR,
        PYTHONUNBUFFERED: '1',
        API_HOST: API_HOST,
        API_PORT: API_PORT,
        METRICS_PORT: METRICS_PORT
      },
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/pm2/api-error.log',
      out_file: './logs/pm2/api-out.log',
      log_file: './logs/pm2/api-combined.log',
      time: true,
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      restart_delay: 4000,
      max_restarts: 10,
      min_uptime: '10s'
    },
    {
      name: 'workflow-engine-executor',
      script: './scripts/start_executor.sh',
      interpreter: 'bash',
      cwd: BASE_DIR,
      env: {
        PYTHONPATH: BASE_DIR,
        PYTHONUNBUFFERED: '1',
        METRICS_PORT: METRICS_PORT
      },
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/pm2/executor-error.log',
      out_file: './logs/pm2/executor-out.log',
      log_file: './logs/pm2/executor-combined.log',
      time: true,
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      restart_delay: 4000,
      max_restarts: 10,
      min_uptime: '10s'
    }
  ]
};

