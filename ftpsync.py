
from ftplib import FTP, all_errors
import posixpath
import utils
from constants import HASH_FILE
import ast
import json
import os
import io
class FtpSync(FTP):
    def __init__(self, localDirectory, remoteDirectory, ip, port = 2121):
        super().__init__()
        self.ip = ip
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
        for subpath in utils.getAllSubpaths(self.rd):
            self.cacheDirs.add(subpath)
        self.initialSync()

    def stop(self):
        print()
        print(self.quit())
    
    def getRemotePath(self, localPath):
        relativePath = os.path.relpath(localPath, self.ld)
        remoteRelativePath = posixpath.sep.join(relativePath.split(os.path.sep))
        remotePath = posixpath.join(self.rd, remoteRelativePath)
        return remotePath

    def initialSync(self):
        # Add non obvious directories like the parent directories of the rd
        
        remoteFiles, remoteDirs = self.recursiveList(self.rd, True)
        for remoteDir in remoteDirs:
            self.cacheDirs.add(remoteDir)
        localFiles  = utils.recursiveLocalList(self.ld)
        hashFiles = {HASH_FILE}

        if hashFiles & remoteFiles:
            hashFilePath = posixpath.join(self.rd, HASH_FILE)
            hashData = bytearray()
            cb = lambda buffer: hashData.extend(buffer)
            self.retrbinary('RETR ' + hashFilePath,cb)
            self.fileHashes = ast.literal_eval(hashData.decode("utf-8"))
        
        filesToCheck = (localFiles & remoteFiles) - hashFiles
        if filesToCheck:
            print("Comparing remote with local files...", end='')

        for fileToCheck in filesToCheck:
            localPath = os.path.join(self.ld, fileToCheck)
            localFileHash = ""
            fd = open(localPath, "rb")
            localFileHash = utils.getMd5(fd)
            remotePath = posixpath.join(self.rd, fileToCheck)
            if self.fileHashes.get(remotePath, "") != localFileHash:
                print("Reuploading...", localFilePath)
                fd.seek(0)
                self.delete(remotePath)
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
        self.retrbinary('RETR ' + remoteFilePath,lambda buffer: fd.write(buffer))
    
    def uploadFile(self, fd, remoteFilePath):
        self.storbinary('STOR ' + remoteFilePath,fd)
    
    def uploadFileByPath(self, localFilePath):
        pass
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
            self.mkd(targetDirectory)
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
