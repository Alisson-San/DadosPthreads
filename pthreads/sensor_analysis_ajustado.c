
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 1024
#define MAX_FIELDS 12
#define MAX_VALID_RECORDS 50000

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
            perror("Erro de alocação");
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

        
        // Verifica se a data é válida (a partir de 2024-03)
        if (strlen(records[record_count].date) >= 7 &&
            strncmp(records[record_count].date, "2024-03", 7) >= 0) {
            
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
    
    }

    fclose(file);
    *data = records;
    return record_count;
}

void print_sample(SensorData *data, int count, int max) {
    printf("\nAmostra dos dados validos a partir de 2024-03 (%d registros):\n", count < max ? count : max);
    for (int i = 0; i < count && i < max; i++) {
        printf("ID: %d | Device: %s | Contagem: %d | Data: %s | Temp: %.2f | Umidade: %.2f | Lum: %.2f | Ruído: %.2f | eCO2: %.2f | eTVOC: %.2f | Lat: %.4f | Long: %.4f\n",
               data[i].id, data[i].device, data[i].count, data[i].date, data[i].temperature, data[i].humidity, data[i].luminosity, data[i].noise, data[i].eco2, data[i].etvoc, data[i].latitude, data[i].longitude);
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
        printf("Nenhum dado válido encontrado.\n");
        return 1;
    }

    print_sample(data, record_count, 5);
    free(data);
    return 0;
}
