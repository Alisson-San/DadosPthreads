#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 1024
#define MAX_FIELDS 12  // Número de colunas no CSV

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

// Função para verificar se uma string está vazia ou só tem espaços
bool is_empty(const char *str) {
    while (*str) {
        if (!isspace((unsigned char)*str)) {
            return false;
        }
        str++;
    }
    return true;
}

// Função para trim de strings (remove espaços no início e fim)
char *trim(char *str) {
    char *end;
    
    // Trim leading space
    while(isspace((unsigned char)*str)) str++;
    
    if(*str == 0)  // String vazia
        return str;
    
    // Trim trailing space
    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;
    
    end[1] = '\0';  // Termina a string
    
    return str;
}

// Função para ler o arquivo CSV
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
        
        // Remove newline no final
        line[strcspn(line, "\n")] = 0;
        
        // Ignora linhas vazias ou só com pipes
        bool empty_line = true;
        for (char *p = line; *p; p++) {
            if (*p != '|' && !isspace(*p)) {
                empty_line = false;
                break;
            }
        }
        if (empty_line) {
            continue;
        }

        // Aloca espaço para o novo registro
        records = realloc(records, (record_count + 1) * sizeof(SensorData));
        if (!records) {
            perror("Erro de alocação");
            fclose(file);
            return -1;
        }

        // Inicializa o registro com valores padrão
        memset(&records[record_count], 0, sizeof(SensorData));
        
        // Faz o parsing da linha
        char *token = strtok(line, "|");
        int field_count = 0;
        
        while (token != NULL && field_count < MAX_FIELDS) {
            char *cleaned_token = trim(token);
            
            // Preenche cada campo conforme a posição
            switch (field_count) {
                case 0:  // id
                    if (!is_empty(cleaned_token)) {
                        records[record_count].id = atoi(cleaned_token);
                    }
                    break;
                case 1:  // device
                    if (!is_empty(cleaned_token)) {
                        strncpy(records[record_count].device, cleaned_token, 
                               sizeof(records[record_count].device) - 1);
                    }
                    break;
                case 2:  // contagem
                    if (!is_empty(cleaned_token)) {
                        records[record_count].count = atoi(cleaned_token);
                    }
                    break;
                case 3:  // data
                    if (!is_empty(cleaned_token)) {
                        strncpy(records[record_count].date, cleaned_token,
                               sizeof(records[record_count].date) - 1);
                    }
                    break;
                case 4:  // temperatura
                    if (!is_empty(cleaned_token)) {
                        records[record_count].temperature = atof(cleaned_token);
                    }
                    break;
                case 5:  // umidade
                    if (!is_empty(cleaned_token)) {
                        records[record_count].humidity = atof(cleaned_token);
                    }
                    break;
                case 6:  // luminosidade
                    if (!is_empty(cleaned_token)) {
                        records[record_count].luminosity = atof(cleaned_token);
                    }
                    break;
                case 7:  // ruido
                    if (!is_empty(cleaned_token)) {
                        records[record_count].noise = atof(cleaned_token);
                    }
                    break;
                case 8:  // eco2
                    if (!is_empty(cleaned_token)) {
                        records[record_count].eco2 = atof(cleaned_token);
                    }
                    break;
                case 9:  // etvoc
                    if (!is_empty(cleaned_token)) {
                        records[record_count].etvoc = atof(cleaned_token);
                    }
                    break;
                case 10: // latitude
                    if (!is_empty(cleaned_token)) {
                        records[record_count].latitude = atof(cleaned_token);
                    }
                    break;
                case 11: // longitude
                    if (!is_empty(cleaned_token)) {
                        records[record_count].longitude = atof(cleaned_token);
                    }
                    break;
            }
            
            token = strtok(NULL, "|");
            field_count++;
        }

        // Verifica se a linha tinha campos suficientes
        if (field_count < MAX_FIELDS) {
            printf("Aviso: Linha %d incompleta (%d campos encontrados)\n", 
                  line_number, field_count);
        }

        record_count++;
    }

    fclose(file);
    *data = records;
    return record_count;
}

// Função para imprimir alguns registros para debug
void print_sample(SensorData *data, int count, int sample_size) {
    if (sample_size > count) sample_size = count;
    
    printf("\nAmostra de %d registros:\n", sample_size);
    printf("-----------------------------------------------------------------\n");
    
    for (int i = 0; i < sample_size; i++) {
        printf("Registro %d:\n", i+1);
        printf("  Device: %s\n", data[i].device);
        printf("  Data: %s\n", data[i].date);
        printf("  Temp: %.2f, Hum: %.2f, Lum: %.2f\n", 
               data[i].temperature, data[i].humidity, data[i].luminosity);
        printf("  Noise: %.2f, eco2: %.2f, etvoc: %.2f\n",
               data[i].noise, data[i].eco2, data[i].etvoc);
        printf("  Coord: (%.6f, %.6f)\n", 
               data[i].latitude, data[i].longitude);
        printf("-----------------------------------------------------------------\n");
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Uso: %s <arquivo_entrada.csv>\n", argv[0]);
        return 1;
    }

    SensorData *data = NULL;
    int record_count = read_csv(argv[1], &data);
    
    if (record_count < 0) {
        return 1;
    }
    
    printf("Lidos %d registros válidos do arquivo %s\n", record_count, argv[1]);
    
    // Imprime uma amostra dos dados para verificação
    print_sample(data, record_count, 3);
    
    // Libera memória alocada
    free(data);
    
    return 0;
}