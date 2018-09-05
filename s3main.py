import boto3, botocore, base64, json, os, sys

SUBMISSIONS = 'submissions'
BUCKET = 'caraza-harter-cs301'
session = boto3.Session(profile_name='cs301ta')
s3 = session.client('s3')

# return all S3 objects with the given key prefix, using as many
# requests as necessary
def s3_all_keys(Prefix):
    ls = s3.list_objects_v2(Bucket=BUCKET, Prefix=Prefix, MaxKeys=10000)
    keys = []
    while True:
        contents = [obj['Key'] for obj in ls.get('Contents',[])]
        keys.extend(contents)
        if not 'NextContinuationToken' in ls:
            break
        ls = s3.list_objects_v2(Bucket=BUCKET,
                                Prefix=Prefix,
                                ContinuationToken=ls['NextContinuationToken'],
                                MaxKeys=10000)
    return keys

def s3_delete_keys(key):
    s3.delete_object(Bucket=BUCKET, Key=key)

def s3Path(cwd):
    return '/' + '/'.join(cwd)

def s3Cd(cwd, command):
    if command[0] == "..":
        if len(cwd) > 0:
            cwd.pop()
        else:
            print("Invalid Operation.")
    else:
        cwd.append(command[0])

def s3Ls(cwd, command):
    path = s3Path(cwd)[1:]
    queryPath = path + "/" if path else ""
    folders = set()
    files = set()
    lineBreak = 0
    for fileName in s3_all_keys(queryPath):
        realPath = fileName[len(queryPath):]
        if "/" in realPath:
            folders.add(realPath.split("/")[0])
        else:
            files.add(realPath)
    for s3folder in sorted(folders):
        print("\033[94m{}\033[0m".format(s3folder), end="\t\t")
        lineBreak += 1
        if lineBreak >= 3:
            lineBreak = 0
            print()
    for s3file in sorted(files):
        print(s3file, end="\t\t")
        lineBreak += 1
        if lineBreak >= 3:
            lineBreak = 0
            print()
    print()
    return folders, files

def s3Rm(cwd, command):
    path = "/".join(cwd) + "/" + command[0]
    s3_delete_keys(path)

def s3Pwd(cwd, command):
    path = s3Path(cwd)
    print(path)

def s3Get(cwd, command):
    path = "/".join(cwd) + "/" + command[0]
    response = s3.get_object(Bucket=BUCKET, Key=path)
    with open(command[0], "w") as fw:
        fw.write(response['Body'].read().decode('utf-8'))

def s3Cat(cwd, command):
    path = "/".join(cwd) + "/" + command[0]
    response = s3.get_object(Bucket=BUCKET, Key=path)
    print(response['Body'].read().decode('utf-8'))

def s3Exit(cwd, command):
    exit()

commandList = {
    "cd" : (s3Cd, 2),
    "ls" : (s3Ls, 1),
    "rm" : (s3Rm, 2),
    "pwd" : (s3Pwd, 1),
    "get" : (s3Get, 2),
    "exit" : (s3Exit, 1),
    "cat" : (s3Cat, 2)
}

def runCommand(currentPath, command):
    if len(command) > 0 and \
        command[0] in commandList and \
        len(command) == commandList[command[0]][1]:
        commandList[command[0]][0](currentPath, command[1:])
    else:
        print("Invalid command.")

if __name__ == "__main__":
    currentPath = []
    while True:
        print(">", end=" ")
        command = input().split()
        runCommand(currentPath, command)
