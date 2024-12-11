#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>

char *basedir = "/storage/ice1/9/3/scorrea34/CS-8803-SII/logs/certificates/ct.googleapis.com_logs_eu1_xenon2024";

int main(int argc, char *argv[]) {
    DIR *dir;
    struct dirent *ent;
    long count = 0;

    dir = opendir(basedir);

    while((ent = readdir(dir))) {
        char cmd[256] = "";
        sprintf(cmd,
                "psql -U scorrea34 -d scorrea34 -c "
                "\"COPY ct_logs FROM '%s/%s' CSV DELIMITER ',';\"",
                basedir, ent->d_name);
        system(cmd);
        printf("%s\n", ent->d_name);
    }

    closedir(dir);

    return 0;
}

