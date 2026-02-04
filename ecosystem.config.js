module.exports = {
  apps: [
    {
      name: 'workflow-engine-api',
      script: 'main.py',
      interpreter: './workflow_venv/bin/python3',
      cwd: '/home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine',
      env: {
        PYTHONPATH: '/home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine',
        PYTHONUNBUFFERED: '1'
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
      script: '-m',
      args: 'executor.main',
      interpreter: './workflow_venv/bin/python3',
      cwd: '/home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine',
      env: {
        PYTHONPATH: '/home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine',
        PYTHONUNBUFFERED: '1'
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

