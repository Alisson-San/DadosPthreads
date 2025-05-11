
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_LINE_LENGTH 1024
#define MAX_FIELDS 12
#define MAX_GROUPS 10000
#define MAX_SENSOR_NAME 20
#define MAX_MONTH 8
#define MAX_DEVICE 50

typedef struct {
    int id;
    char device[50];
    int count;
    char date[20];
    float temperature;
    float humidity;
    float luminosity;
    float noise;
    float eco2;
    float etvoc;
    float latitude;
    float longitude;
} SensorData;

typedef struct {
    char device[MAX_DEVICE];
    char month[MAX_MONTH];
    char sensor[MAX_SENSOR_NAME];
    float min;
    float max;
    float sum;
    int count;
} SensorStats;

typedef struct {
    SensorData *data;
    int start;
    int end;
    SensorStats *local_stats;
    int *local_count;
} ThreadArgs;

bool is_empty(const char *str) {
    while (*str) {
        if (!isspace((unsigned char)*str)) return false;
        str++;
    }
    return true;
}

char *trim(char *str) {
    char *end;
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return str;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}

int is_same_group(SensorStats *a, const char *device, const char *month, const char *sensor) {
    return strcmp(a->device, device) == 0 && strcmp(a->month, month) == 0 && strcmp(a->sensor, sensor) == 0;
}

void* thread_worker(void* arg) {
    ThreadArgs *args = (ThreadArgs*) arg;
    SensorData *data = args->data;
    SensorStats *stats = args->local_stats;
    int group_count = 0;

    for (int i = args->start; i < args->end; i++) {
        SensorData s = data[i];
        char month[8];
        strncpy(month, s.date, 7);
        month[7] = '\0';

        struct {
            const char *name;
            float value;
        } sensors[] = {
            {"temperature", s.temperature},
            {"humidity", s.humidity},
            {"luminosity", s.luminosity},
            {"noise", s.noise},
            {"eco2", s.eco2},
            {"etvoc", s.etvoc}
        };

        for (int j = 0; j < 6; j++) {
            int found = 0;
            for (int k = 0; k < group_count; k++) {
                if (is_same_group(&stats[k], s.device, month, sensors[j].name)) {
                    if (sensors[j].value < stats[k].min) stats[k].min = sensors[j].value;
                    if (sensors[j].value > stats[k].max) stats[k].max = sensors[j].value;
                    stats[k].sum += sensors[j].value;
                    stats[k].count++;
                    found = 1;
                    break;
                }
            }
            if (!found && group_count < MAX_GROUPS) {
                SensorStats *g = &stats[group_count++];
                strncpy(g->device, s.device, MAX_DEVICE);
                strncpy(g->month, month, MAX_MONTH);
                strncpy(g->sensor, sensors[j].name, MAX_SENSOR_NAME);
                g->min = g->max = g->sum = sensors[j].value;
                g->count = 1;
            }
        }
    }

    *args->local_count = group_count;
    return NULL;
}

void salvar_csv(SensorStats *stats, int total, const char *nome_arquivo) {
    FILE *fp = fopen(nome_arquivo, "w");
    if (!fp) {
        perror("Erro ao criar arquivo de saida");
        return;
    }

    fprintf(fp, "device;ano-mes;sensor;valor_maximo;valor_medio;valor_minimo\n");
    for (int i = 0; i < total; i++) {
        SensorStats *g = &stats[i];
        float media = g->sum / g->count;
        fprintf(fp, "%s;%s;%s;%.2f;%.2f;%.2f\n", g->device, g->month, g->sensor, g->max, media, g->min);
    }

    fclose(fp);
    printf("\nArquivo de resultados salvo como '%s'\n", nome_arquivo);
}

int read_csv(const char *filename, SensorData **data) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Erro ao abrir arquivo");
        return -1;
    }

    char line[MAX_LINE_LENGTH];
    SensorData *records = NULL;
    int record_count = 0;

    while (fgets(line, sizeof(line), file)) {
        line[strcspn(line, "\n")] = 0;

        char line_copy[MAX_LINE_LENGTH];
        strncpy(line_copy, line, MAX_LINE_LENGTH);
        line_copy[MAX_LINE_LENGTH - 1] = '\0';

        int field_count = 0;
        char *tok = strtok(line_copy, "|");
        while (tok) {
            field_count++;
            tok = strtok(NULL, "|");
        }

        if (field_count != MAX_FIELDS) continue;

        records = realloc(records, (record_count + 1) * sizeof(SensorData));
        if (!records) {
            perror("Erro de alocacao");
            fclose(file);
            return -1;
        }

        memset(&records[record_count], 0, sizeof(SensorData));
        char *token = strtok(line, "|");
        for (int i = 0; i < MAX_FIELDS && token != NULL; i++) {
            char *clean = trim(token);
            switch (i) {
                case 0: if (!is_empty(clean)) records[record_count].id = atoi(clean); break;
                case 1: if (!is_empty(clean)) strncpy(records[record_count].device, clean, sizeof(records[record_count].device) - 1); break;
                case 2: if (!is_empty(clean)) records[record_count].count = atoi(clean); break;
                case 3: if (!is_empty(clean)) strncpy(records[record_count].date, clean, sizeof(records[record_count].date) - 1); break;
                case 4: if (!is_empty(clean)) records[record_count].temperature = atof(clean); break;
                case 5: if (!is_empty(clean)) records[record_count].humidity = atof(clean); break;
                case 6: if (!is_empty(clean)) records[record_count].luminosity = atof(clean); break;
                case 7: if (!is_empty(clean)) records[record_count].noise = atof(clean); break;
                case 8: if (!is_empty(clean)) records[record_count].eco2 = atof(clean); break;
                case 9: if (!is_empty(clean)) records[record_count].etvoc = atof(clean); break;
                case 10: if (!is_empty(clean)) records[record_count].latitude = atof(clean); break;
                case 11: if (!is_empty(clean)) records[record_count].longitude = atof(clean); break;
            }
            token = strtok(NULL, "|");
        }

        if (strlen(records[record_count].date) >= 7 &&
            strncmp(records[record_count].date, "2024-03", 7) >= 0) {
            record_count++;
        }
    }

    fclose(file);
    *data = records;
    return record_count;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Uso: %s <arquivo_entrada.csv>\n", argv[0]);
        return 1;
    }

    SensorData *data = NULL;
    int record_count = read_csv(argv[1], &data);
    if (record_count <= 0) {
        printf("Nenhum dado valido encontrado.\n");
        return 1;
    }

    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    pthread_t threads[num_threads];
    ThreadArgs args[num_threads];
    SensorStats *thread_stats[num_threads];
    int local_counts[num_threads];

    int bloco = record_count / num_threads;

    for (int i = 0; i < num_threads; i++) {
        int start = i * bloco;
        int end = (i == num_threads - 1) ? record_count : start + bloco;

        thread_stats[i] = calloc(MAX_GROUPS, sizeof(SensorStats));
        args[i].data = data;
        args[i].start = start;
        args[i].end = end;
        args[i].local_stats = thread_stats[i];
        args[i].local_count = &local_counts[i];

        pthread_create(&threads[i], NULL, thread_worker, &args[i]);
    }

    SensorStats merged[MAX_GROUPS];
    int merged_count = 0;

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);

        for (int j = 0; j < local_counts[i]; j++) {
            SensorStats *s = &thread_stats[i][j];
            int found = 0;
            for (int k = 0; k < merged_count; k++) {
                if (is_same_group(&merged[k], s->device, s->month, s->sensor)) {
                    if (s->min < merged[k].min) merged[k].min = s->min;
                    if (s->max > merged[k].max) merged[k].max = s->max;
                    merged[k].sum += s->sum;
                    merged[k].count += s->count;
                    found = 1;
                    break;
                }
            }
            if (!found && merged_count < MAX_GROUPS) {
                merged[merged_count++] = *s;
            }
        }

        free(thread_stats[i]);
    }

    salvar_csv(merged, merged_count, "resultados.csv");
    free(data);
    return 0;
}
