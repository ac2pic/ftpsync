#!/usr/bin/python3
import os
import sys
import time
import logging
import hashlib
import json
import io
import ast
from os.path import relpath, join
from ftplib import FTP, all_errors
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from logging import StreamHandler
import posixpath
from functools import reduce
from operator import concat 

def recursiveLocalList(baseDir):
    localFiles = set()
    for root, dirs, files in os.walk(baseDir):
        for file in files:
            if relpath(root, baseDir) == '.':
                localFiles.add(file)
            else:
                localFiles.add(join(relpath(root, baseDir), file))
    return localFiles

def getAllSubpaths(filePath):
    subpaths = set()
    pathPieces = filePath.split(posixpath.sep)
    rootPathPieces = []
    for pathPiece in pathPieces:
        rootPathPieces.append(pathPiece)
        if pathPiece == "":
            continue
        subpaths.add(posixpath.sep.join(rootPathPieces))
    return subpaths

def getMd5(fd):
    CHUNK_SIZE = 4096 * 2
    m = hashlib.md5()
    while True:
        chunk = fd.read(CHUNK_SIZE)
        if chunk == b"":
            break
        m.update(chunk) 
    return m.hexdigest()

HASH_FILE = '.MD5HASHES'

class FtpSync(FTP):
    def __init__(self, localDirectory, remoteDirectory, ip, port = 2121):
        super().__init__()
        self.ip = IP
        self.port = port
        self.ld = localDirectory
        self.rd = remoteDirectory
        self.cacheDirs = set()
        self.fileHashes = {}

    def connect(self):
        super().connect(self.ip, self.port)
    
    def start(self):
        self.connect()
        print(self.login())
        print(self.getwelcome())
        for subpath in getAllSubpaths(self.rd):
            self.cacheDirs.add(subpath)
        self.initialSync()

    def stop(self):
        print()
        print(self.quit())

    def initialSync(self):
        # Add non obvious directories like the parent directories of the rd
        
        remoteFiles, remoteDirs = self.recursiveList(self.rd, True)
        for remoteDir in remoteDirs:
            self.cacheDirs.add(remoteDir)
        localFiles  = recursiveLocalList(self.ld)
        hashFiles = {HASH_FILE}

        if hashFiles & remoteFiles:
            hashFilePath = posixpath.join(self.rd, HASH_FILE)
            hashData = bytearray()
            cb = lambda buffer: hashData.extend(buffer)
            ftp.retrbinary('RETR ' + hashFilePath,cb)
            self.fileHashes = ast.literal_eval(hashData.decode("utf-8"))
        
        filesToCheck = (localFiles & remoteFiles) - hashFiles
        if filesToCheck:
            print("Comparing remote with local files...", end='')
        for fileToCheck in filesToCheck:
            localPath = os.path.join(self.ld, fileToCheck)
            localFileHash = ""
            fd = open(localPath, "rb")
            localFileHash = getMd5(fd)
            remotePath = posixpath.join(self.rd, fileToCheck)
            if self.fileHashes.get(remotePath, "") != localFileHash:
                print("Reuploading...", localFilePath)
                fd.seek(0)
                self.uploadFile(fd, remotePath)
                self.fileHashes[remotePath] = localFileHash
        if filesToCheck:
            print("Done")

        filesToDownload = remoteFiles - (localFiles | hashFiles)
        if filesToDownload:
            print("Downloading files...", end='')

        for fileToDownload in filesToDownload:
            remotePath = posixpath.join(self.rd, fileToDownload)
            localPath = os.path.join(self.ld, fileToDownload)
            os.makedirs(os.path.dirname(localPath), exist_ok=True)
            with open(localPath, 'wb') as fd:
                fd.seek(0)
                self.downloadFile(fd, remotePath)
                fd.seek(0)
                self.registerHash(remotePath, fd)
        if filesToDownload:
            print("Done")

        filesToUpload = localFiles - (remoteFiles | hashFiles)
        if filesToUpload:
           print("Uploading files...", end='') 
        for fileToUpload in filesToUpload:
            # Recursively make directory if it doesn't exist
            localPath = join(self.ld, fileToUpload)
            remotePath = posixpath.join(self.rd, fileToUpload)
            remoteDirectory = posixpath.dirname(remotePath)
            self.recursiveMkdir(remoteDirectory)
            with open(localPath, 'rb') as fd:
                fd.seek(0)
                self.uploadFile(fd, remotePath)
                fd.seek(0)
                self.registerHash(remotePath, fd)
        if filesToUpload:
           print("Done")

        self.syncFileHashes()

    def registerHash(self, remoteFilePath, fd):
        self.fileHashes[remoteFilePath] = getMd5(fd)
    
    def syncFileHashes(self):
        print("Uploading file hashes...", end='')
        fileHashesStr = json.dumps(self.fileHashes)
        fd = io.BytesIO(bytearray(fileHashesStr, "utf-8")) 
        remotePath = posixpath.join(self.rd, HASH_FILE)
        self.uploadFile(fd, remotePath)
        print("Done")

    def downloadFile(self, fd, remoteFilePath):
        ftp.retrbinary('RETR ' + remoteFilePath,lambda buffer: fd.write(buffer))
    
    def uploadFile(self, fd, remoteFilePath):
        ftp.storbinary('STOR ' + remoteFilePath,fd)

    def recursiveMkdir(self, remotePath, cacheDirs = set()):
        pathPieces = remotePath.split(posixpath.sep)
        rootDirPieces = []
        for pathPiece in pathPieces:
            rootDirPieces.append(pathPiece)
            if pathPiece == '':
                continue

            targetDirectory = posixpath.sep.join(rootDirPieces)
            if targetDirectory in self.cacheDirs:
                # Already exists to skip it
                continue
            print("Making directory...", targetDirectory)
            ftp.mkd(targetDirectory)
            # Add to prevent remaking it again
            self.cacheDirs.add(targetDirectory)

    def recursiveList(self, remotePath, relativePath = False):
        files = set()
        dirs = set()
        toExplore = [remotePath]
        while len(toExplore) > 0:
            rootDir = toExplore.pop()
            dirFiles = self.mlsd(rootDir)
            try:
                # This is a cdir so we can ignore it anyway
                next(dirFiles)
            except all_errors as e:
                continue
            for (name, data) in dirFiles:                                 
                fileName = posixpath.join(rootDir, name)
                 
                if data['type'] == 'dir':
                    dirs.add(fileName)
                    toExplore.append(fileName)
                elif data['type'] == 'file':
                    if relativePath:
                        fileName = fileName.replace(remotePath, "")
                        if fileName[0] == '/':
                            fileName = fileName[1:]

                    files.add(fileName)
        return files, dirs

class SyncEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""
    
    def __init__(self):
        super().__init__()
        logging.getLogger().addHandler(StreamHandler(sys.stdout))
        self.logger = logging.getLogger()

    def on_moved(self, event: FileSystemEvent) -> None:
        super().on_moved(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)

    def on_created(self, event: FileSystemEvent) -> None:
        super().on_created(event)

        what = "directory" if event.is_directory else "file"
        self.logger.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event: FileSystemEvent) -> None:
        super().on_deleted(event)
        what = "directory" if event.is_directory else "file"
        self.logger.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        super().on_modified(event)
        if event.is_directory:
            return
        what = "directory" if event.is_directory else "file"

        self.logger.info("Modified %s: %s", what, event.src_path)

    def on_closed(self, event: FileSystemEvent) -> None:
        super().on_closed(event)
        self.logger.info("Closed file: %s", event.src_path)

    def on_opened(self, event: FileSystemEvent) -> None:
        super().on_opened(event)
        self.logger.info("Opened file: %s", event.src_path)

if __name__ == "__main__":

    IP = "10.0.0.5"
    PORT = 2121
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    if len(sys.argv) < 3:
        print(sys.argv[0], " localDirectory remoteDirectory")
        exit(-1)
    localDirectory = sys.argv[1] if len(sys.argv) > 1 else '.'
    remoteDirectory = sys.argv[2]  
    ftp = FtpSync(localDirectory, remoteDirectory, IP, PORT)
    ftp.start()
    event_handler = SyncEventHandler()
    observer = Observer()
    observer.schedule(event_handler, localDirectory, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        ftp.stop()
        observer.stop()
        observer.join()

