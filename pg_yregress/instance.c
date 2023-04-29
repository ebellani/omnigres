#include <assert.h>
#include <errno.h>
#include <ftw.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "pg_yregress.h"

default_yinstance_result default_instance(struct fy_node *instances, yinstance **instance) {
  switch (fy_node_mapping_item_count(instances)) {
  case 0:
    // There are no instances available
    return default_instance_not_found;
  case 1:
    // There is only one available, that'll be default
    *instance = (yinstance *)fy_node_get_meta(
        fy_node_pair_value(fy_node_mapping_get_by_index(instances, 0)));
    return default_instance_found;

  default:
    // There are more than one instance to choose from
    {
      // Try to see if any is set as default
      void *iter = NULL;

      struct fy_node_pair *instance_pair;
      while ((instance_pair = fy_node_mapping_iterate(instances, &iter)) != NULL) {
        struct fy_node *instance_node = fy_node_pair_value(instance_pair);
        yinstance *y_instance = (yinstance *)fy_node_get_meta(instance_node);
        if (y_instance->is_default) {
          *instance = y_instance;
          return default_instance_found;
        }
      }
    }
    return default_instance_ambiguous;
  }
}

iovec_t yinstance_name(yinstance *instance) {
  if (instance->name.base != NULL) {
    return instance->name;
  } else {
    char *path = fy_node_get_path(instance->node);
    return (iovec_t){.base = path, .len = strlen(path)};
  }
}

static uint16_t get_available_inet_port() {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return 0;
  }

  struct sockaddr_in addr;
  bzero((char *)&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = 0;

  if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    return 0;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(sock, (struct sockaddr *)&addr, &len) < 0) {
    return 0;
  }

  uint16_t port = ntohs(addr.sin_port);

  if (close(sock) < 0) {
    return 0;
  }

  return port;
}

yinstance_connect_result yinstance_connect(yinstance *instance) {
  if (instance->conn != NULL && PQstatus(instance->conn) != CONNECTION_BAD) {
    return yinstance_connect_success;
  }
  if (instance->managed) {
    char *conninfo;
    asprintf(&conninfo, "host=127.0.0.1 port=%d dbname=yregress user=yregress",
             instance->info.managed.port);
    instance->conn = PQconnectdb(conninfo);

    // Prepare the database if connected
    if (PQstatus(instance->conn) == CONNECTION_OK) {
      // Initialize
      if (fy_node_is_mapping(instance->node)) {
        struct fy_node *init = fy_node_mapping_lookup_by_string(instance->node, STRLIT("init"));
        if (init != NULL) {
          assert(fy_node_is_sequence(init));
          {
            void *iter = NULL;
            struct fy_node *step;
            int cur_step = 0;
            while ((step = fy_node_sequence_iterate(init, &iter)) != NULL) {
              if (cur_step >= instance->init_step) {
                switch (fy_node_get_type(step)) {
                case FYNT_SCALAR: {
                  // Statement
                  size_t len;
                  const char *stmt0 = fy_node_get_scalar(step, &len);
                  char *stmt;
                  asprintf(&stmt, "%.*s", (int)len, stmt0);
                  if (PQresultStatus(PQexec(instance->conn, stmt)) != PGRES_COMMAND_OK) {
                    // TODO: error
                  }
                  break;
                }
                case FYNT_MAPPING: {
                  struct fy_node *restart =
                      fy_node_mapping_lookup_by_string(step, STRLIT("restart"));
                  if (restart != NULL && fy_node_is_scalar(restart)) {
                    PQfinish(instance->conn);
                    instance->conn = NULL;
                    // Restart from the next step
                    instance->init_step++;
                    return yinstance_connect_restart;
                  }
                  break;
                }
                default: {
                  // do nothing
                }
                }
              }
              cur_step++;
            }
          }
        }
      }
    }
    return yinstance_connect_success;
  }

  return yinstance_connect_failure;
}

void yinstance_start(yinstance *instance) {
  if (instance->managed) {

    char datadir[] = "pg_yregress_XXXXXX";
    mkdtemp(datadir);

    // Initialize the cluster
    char *initdb_command;
    asprintf(&initdb_command,
             "%s/pg_ctl initdb -o '-A trust -U yregress --no-clean --no-sync' -s -D %s", bindir,
             datadir);
    system(initdb_command);

    // Add configuration
    struct fy_node *config = fy_node_mapping_lookup_by_string(instance->node, STRLIT("config"));
    if (config != NULL) {
      char *config_file;
      asprintf(&config_file, "%s/postgresql.auto.conf", datadir);
      if (fy_node_is_scalar(config)) {
        FILE *cfg = fopen(config_file, "w");
        fprintf(cfg, "%s\n", fy_node_get_scalar(config, NULL));
        fclose(cfg);
      } else if (fy_node_is_mapping(config)) {
        FILE *cfg = fopen(config_file, "w");
        {
          void *iter = NULL;
          struct fy_node_pair *cfg_pair;
          while ((cfg_pair = fy_node_mapping_iterate(config, &iter)) != NULL) {
            struct fy_node *key = fy_node_pair_key(cfg_pair);
            struct fy_node *value = fy_node_pair_value(cfg_pair);
            size_t keylen, valuelen;
            if (fy_node_is_scalar(key) && fy_node_is_scalar(value)) {
              const char *keystring = fy_node_get_scalar(key, &keylen);
              const char *valuestring = fy_node_get_scalar(value, &valuelen);
              fprintf(cfg, "%.*s = '%.*s'\n", (int)keylen, keystring, (int)valuelen, valuestring);
            }
          }
        }
        fclose(cfg);
      }
    }

    // Start the database
    instance->info.managed.port = get_available_inet_port();

    // Prepare to initialize from the beginning. If restart is requested,
    // this will be used to resume from a different step.
    instance->init_step = 0;

    char *start_command;
    asprintf(&start_command, "%s/pg_ctl start -o '-c port=%d' -D %s -s", bindir,
             instance->info.managed.port, datadir);
    system(start_command);

    // Create the database
    char *createdb_command;
    asprintf(&createdb_command, "%s/createdb -U yregress -O yregress -p %d yregress", bindir,
             instance->info.managed.port);
    system(createdb_command);

    // Wait until it is ready
    ConnStatusType status;
    bool ready = false;
    while (!ready) {
      switch (yinstance_connect(instance)) {
      case yinstance_connect_success:
        ready = true;
        break;
      case yinstance_connect_failure:
        if (PQstatus(instance->conn) == CONNECTION_BAD) {
          fprintf(stderr, "can't connect: %s\n", PQerrorMessage(instance->conn));
          ready = true;
          break;
        }
        break;
      case yinstance_connect_restart: {
        char *restart_command;
        asprintf(&restart_command, "%s/pg_ctl restart -o '-c port=%d' -D %s -s", bindir,
                 instance->info.managed.port, datadir);
        system(restart_command);
      }
      }
    }

    char *heap_datadir = strdup(datadir);
    instance->info.managed.datadir = (iovec_t){.base = heap_datadir, .len = strlen(heap_datadir)};

    // Get postmaster PID
    {
      char *pid_filename;
      asprintf(&pid_filename, "%s/postmaster.pid", datadir);
      FILE *fp = fopen(pid_filename, "r");
      assert(fp);
      char pid_str[32];
      assert(fgets(pid_str, sizeof(pid_str), fp));
      instance->pid = atol(pid_str);
      assert(errno == 0);
      free(pid_filename);
      fclose(fp);
    }

    // Link postmaster into our process group
    setpgid(instance->pid, pgid);

    instance->ready = true;
  }
}

static int remove_entry(const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  return remove(path);
}

void instances_cleanup() {
  if (instances != NULL) {
    void *iter = NULL;
    struct fy_node_pair *instance_pair;

    while ((instance_pair = fy_node_mapping_iterate(instances, &iter)) != NULL) {
      struct fy_node *instance = fy_node_pair_value(instance_pair);
      yinstance *y_instance = (yinstance *)fy_node_get_meta(instance);

      if (y_instance->ready) {
        if (y_instance->managed) {
          char *stop_command;
          asprintf(&stop_command, "%s/pg_ctl stop -D %.*s -m immediate -s", bindir,
                   (int)IOVEC_STRLIT(y_instance->info.managed.datadir));
          system(stop_command);

          // Cleanup the directory
          nftw(strndup(IOVEC_RSTRLIT(y_instance->info.managed.datadir)), remove_entry, FOPEN_MAX,
               FTW_DEPTH | FTW_PHYS);
        }
        y_instance->ready = false;
      }
    }
  }
}

void register_instances_cleanup() { atexit(instances_cleanup); }
