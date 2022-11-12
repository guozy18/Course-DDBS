import argparse
import os
import re

USER_SQL_FILE = 'user.sql'
USER_READ_SQL_FILE = 'user_read.sql'
ARTICLE_SQL_FILE = 'article.sql'

UID_INDEX_IN_USER = 2
REGION_INDEX_IN_USER = 10

AID_INDEX_IN_ARTICLE = 2
CATEGORY_INDEX_IN_ARTICLE = 4

UID_INDEX_IN_USER_READ = 2

def generate_user_shard(args):
    # Beijing
    s1_uid = []
    # Hongkong
    s2_uid = []
    src_path = os.path.join(args.source, USER_SQL_FILE)
    s1_path = os.path.join(args.source, 'user_shard1.sql')
    s2_path = os.path.join(args.source, 'user_shard2.sql')
    src = open(src_path, 'r')
    s1 = open(s1_path, 'w')
    s2 = open(s2_path, 'w')

    while True:
        line = src.readline()
        s1.write(line)
        s2.write(line)
        if re.search(r'^INSERT INTO', line):
            break

    while True:
        line = src.readline()
        entry = line.split(', ')
        if entry[REGION_INDEX_IN_USER] == '"Beijing"':
            s1.write(line)
            s1_uid.append(entry[UID_INDEX_IN_USER])
        else:
            assert(entry[REGION_INDEX_IN_USER] == '"Hong Kong"')
            s2.write(line)
            s2_uid.append(entry[UID_INDEX_IN_USER])
        # uid is in 
        if re.search(r';$', line.strip()):
            break
    
    while True:
        line = src.readline()
        if not line:
            break
        else:
            s1.write(line)
            s2.write(line)

    src.close()
    s1.close()
    s2.close()
    return (s1_uid, s2_uid)

def generate_article_shard(args):
    s1_aid = []
    s2_aid = []
    src_path = os.path.join(args.source, ARTICLE_SQL_FILE)
    s1_path = os.path.join(args.source, 'article_shard1.sql')
    s2_path = os.path.join(args.source, 'article_shard2.sql')
    src = open(src_path, 'r')
    s1 = open(s1_path, 'w')
    s2 = open(s2_path, 'w')

    while True:
        line = src.readline()
        s1.write(line)
        s2.write(line)
        if re.search(r'^INSERT INTO', line):
            break

    while True:
        line = src.readline()
        entry = line.split(', ')
        s1.write(line)
        s1_aid.append(entry[AID_INDEX_IN_ARTICLE])
        if entry[CATEGORY_INDEX_IN_ARTICLE] == '"technology"':
            s2.write(line)
            s2_aid.append(entry[AID_INDEX_IN_ARTICLE])
        # uid is in 
        if re.search(r';$', line.strip()):
            break
    
    while True:
        line = src.readline()
        if not line:
            break
        else:
            s1.write(line)
            s2.write(line)

    src.close()
    s1.close()
    s2.close()


def generate_user_read_shard(args, s1_uid, s2_uid):
    src_path = os.path.join(args.source, USER_READ_SQL_FILE)
    s1_path = os.path.join(args.source, 'user_read_shard1.sql')
    s2_path = os.path.join(args.source, 'user_read_shard2.sql')
    src = open(src_path, 'r')
    s1 = open(s1_path, 'w')
    s2 = open(s2_path, 'w')

    s1_uid = set(s1_uid)
    s2_uid = set(s2_uid)

    while True:
        line = src.readline()
        s1.write(line)
        s2.write(line)
        if re.search(r'^INSERT INTO', line):
            break

    while True:
        line = src.readline()
        entry = line.split(', ')
        if entry[UID_INDEX_IN_USER_READ] in s1_uid:
            s1.write(line)
        else:
            assert(entry[UID_INDEX_IN_USER_READ] in s2_uid)
            s2.write(line)
        # uid is in 
        if re.search(r';$', line.strip()):
            break
    
    while True:
        line = src.readline()
        if not line:
            break
        else:
            s1.write(line)
            s2.write(line)

    src.close()
    s1.close()
    s2.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True, type=str, help="source directory contains the .sql and article folder")
    args = parser.parse_args()
    (s1_uid, s2_uid) = generate_user_shard(args)
    generate_article_shard(args)
    generate_user_read_shard(args, s1_uid, s2_uid)
