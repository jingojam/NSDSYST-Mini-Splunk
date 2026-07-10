## Docker Commands
- Run `sudo docker compose up -d` to start docker compose containers
- Run `sudo docker compose ps` to inspect running containers
- Run `sudo docker compose stop` to stop running containers

## VM Config
Initializing elasticsearch containers might encounter vMem errors, run:
- `grep vm.max_map_count /etc/sysctl.conf`
- `vm.max_map_count=262144`
