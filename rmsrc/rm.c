/*
 * Implements the rm command
 *
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

static int force = 0;

static int unlink_cb(const char *path, const struct stat *sb,
                     int typeflag, struct FTW *ftwbuf)
{
    int ret;

    (void)sb;
    (void)ftwbuf;

    if (typeflag == FTW_DP)
        ret = rmdir(path);
    else
        ret = unlink(path);

    if (ret != 0 && !force) {
        perror(path);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    int recursive = 0;
    int opt;

    while ((opt = getopt(argc, argv, "rf")) != -1) {
        if (opt == 'r') recursive = 1;
        else if (opt == 'f') force = 1;
        else {
            fprintf(stderr, "usage: rm [-r] [-f] file...\n");
            return 1;
        }
    }

    if (optind >= argc)
        return 0;

    for (int i = optind; i < argc; i++) {
        if (recursive) {
            if (nftw(argv[i], unlink_cb, 64, FTW_DEPTH | FTW_PHYS) != 0 && !force)
                return 1;
        } else {
            if (unlink(argv[i]) != 0 && !force) {
                perror(argv[i]);
                return 1;
            }
        }
    }

    return 0;
}
