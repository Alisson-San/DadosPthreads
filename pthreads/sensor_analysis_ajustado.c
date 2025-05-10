#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 1024
#define MAX_FIELDS 12
#define MAX_VALID_RECORDS 50000

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
    char month[MAX_MONTH];           // Ex: "2024-03"
    char sensor[MAX_SENSOR_NAME];   // Ex: "temperature", "humidity" etc.
    float min;
    float max;
    float sum;
    int count;
} SensorStats;

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

void process_stats(SensorData *data, int record_count) {
    SensorStats stats[MAX_GROUPS];
    int group_count = 0;

    for (int i = 0; i < record_count; i++) {
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

    printf("\nResumo estatistico (agrupado por dispositivo, mes e sensor):\n");
    for (int i = 0; i < group_count; i++) {
        SensorStats *g = &stats[i];
        float media = g->sum / g->count;
        printf("Device: %s | Mes: %s | Sensor: %s | Min: %.2f | Max: %.2f | Media: %.2f\n",
               g->device, g->month, g->sensor, g->min, g->max, media);
    }
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
    int line_number = 0;

    while (fgets(line, sizeof(line), file)) {
        line_number++;
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

        if (field_count != MAX_FIELDS) {
            continue;
        }

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
            if (record_count >= MAX_VALID_RECORDS) break;
            if (record_count % 1000 == 0) {
                printf("Lidas %d linhas validas ate agora...\n", record_count);
                fflush(stdout);
            }
        }
    }

    fclose(file);
    *data = records;
    return record_count;
}

void print_sample(SensorData *data, int count, int max) {
    printf("\nAmostra dos dados validos a partir de 2024-03 (%d registros):\n", count < max ? count : max);
    for (int i = 0; i < count && i < max; i++) {
        printf("ID: %d | Device: %s | Contagem: %d | Data: %s | Temp: %.2f | Umidade: %.2f | Lum: %.2f | Ruido: %.2f | eCO2: %.2f | eTVOC: %.2f | Lat: %.4f | Long: %.4f\n",
               data[i].id, data[i].device, data[i].count, data[i].date, data[i].temperature, data[i].humidity,
               data[i].luminosity, data[i].noise, data[i].eco2, data[i].etvoc, data[i].latitude, data[i].longitude);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Uso: %s <arquivo_entrada.csv>\n", argv[0]);
        return 1;
    }

    SensorData *data = NULL;
    printf("Iniciando leitura do arquivo...\n");
    int record_count = read_csv(argv[1], &data);
    printf("Leitura concluida. Total de registros definidos para leitura: %d\n", record_count);

    if (record_count <= 0) {
        printf("Nenhum dado valido encontrado.\n");
        return 1;
    }

    print_sample(data, record_count, 5);
    process_stats(data, record_count);

    free(data);
    return 0;
}
