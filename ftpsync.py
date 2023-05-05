
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
        self.printers = []

    def addPrinter(self, printer):
        self.printers.append(printer)

    def print(self, msg, end='\n'):
        for printer in self.printers:
            printer(msg, end = end)

    def connect(self):
        super().connect(self.ip, self.port)
    
    def start(self):
        self.print("Logging in..")
        self.connect()
        self.print(self.login())
        self.print(self.getwelcome())
        for subpath in utils.getAllSubpaths(self.rd):
            self.cacheDirs.add(subpath)
        self.print("Syncing up files...")
        self.initialSync()
        self.print("Synced")

    def stop(self):
        self.print("")
        self.print(self.quit())

    def initialSync(self):
        # Add non obvious directories like the parent directories of the rd
        
        remoteFiles, remoteDirs = self.recursiveList(self.rd, True)
        for remoteDir in remoteDirs:
            self.cacheDirs.add(remoteDir)

        localFiles, localDirectories  = utils.recursiveLocalList(self.ld)

        for localDirectory in localDirectories:
            remoteDir = self.getRemotePath(localDirectory)
            if remoteDir not in remoteDirs:
                self.print("Creating directory {}".format(remoteDir))
                self.recursiveMkdir(remoteDir)
        
        for remoteDirectory in remoteDirs:
            localDir = self.getLocalPath(remoteDirectory)
            if localDir not in localDirectories:
                self.print("Creating directory {}".format(localDir))
                os.makedirs(localDir, exist_ok=True)

        hashFiles = {HASH_FILE}

        if hashFiles & remoteFiles:
            hashFilePath = posixpath.join(self.rd, HASH_FILE)
            hashData = bytearray()
            cb = lambda buffer: hashData.extend(buffer)
            self.retrbinary('RETR ' + hashFilePath,cb)
            self.fileHashes = ast.literal_eval(hashData.decode("utf-8"))
        
        filesToCheck = (localFiles & remoteFiles) - hashFiles
        for fileToCheck in filesToCheck:
            localPath = os.path.join(self.ld, fileToCheck)
            self.uploadIfHashChanged(localPath)

        filesToDownload = remoteFiles - (localFiles | hashFiles)
        for fileToDownload in filesToDownload:
            remotePath = posixpath.join(self.rd, fileToDownload)
            localPath = os.path.join(self.ld, fileToDownload)
            self.downloadFileByPath(localPath)
            self.registerHash(remotePath, utils.getFileMd5(localPath))


        filesToUpload = localFiles - (remoteFiles | hashFiles)
        for fileToUpload in filesToUpload:
            localPath = os.path.join(self.ld, fileToUpload)
            remotePath = posixpath.join(self.rd, fileToUpload)
            self.uploadFileByPath(localPath)
            self.registerHash(remotePath, utils.getFileMd5(localPath))

        self.syncFileHashes()
    
    def getRemotePath(self, localPath):
        relativePath = os.path.relpath(localPath, self.ld)
        remoteRelativePath = posixpath.sep.join(relativePath.split(os.path.sep))
        remotePath = posixpath.join(self.rd, remoteRelativePath)
        return remotePath
    
    def getLocalPath(self, remotePath):
        relativePath = posixpath.relpath(remotePath, self.rd)
        localRelativePath = os.sep.join(relativePath.split(posixpath.sep))
        localPath = os.path.join(self.ld, localRelativePath)
        return localPath

    def uploadIfHashChanged(self, localPath):
        localHash = utils.getFileMd5(localPath)
        remotePath = self.getRemotePath(localPath)
        if self.getHash(remotePath) == localHash:
            return False
        if self.getHash(remotePath):
            self.print("Updating {}".format(remotePath))
            self.delete(remotePath)
        else:
            self.print("Creating {}".format(remotePath))

        with open(localPath, "rb") as fh:
            remoteDirectory = posixpath.dirname(remotePath)
            self.recursiveMkdir(remoteDirectory)
            self.uploadFile(fh, remotePath)
            self.registerHash(remotePath, localHash)

        return True


    def registerHash(self, remotePath, fileHash):
        self.fileHashes[remotePath] = fileHash

    def getHash(self, remotePath):
        return self.fileHashes.get(remotePath, "")

    def unregisterHash(self, remotePath):
        del self.fileHashes[remotePath]

    def syncFileHashes(self):
        fileHashesStr = json.dumps(self.fileHashes)
        fh = io.BytesIO(bytearray(fileHashesStr, "utf-8")) 
        remotePath = posixpath.join(self.rd, HASH_FILE)
        self.uploadFile(fh, remotePath)

    def downloadFile(self, fh, remotePath):
        self.retrbinary('RETR ' + remotePath,lambda buffer: fh.write(buffer))
    
    def uploadFile(self, fh, remotePath):
        self.storbinary('STOR ' + remotePath,fh)
    

    def uploadFileByPath(self, localPath):
        remotePath = self.getRemotePath(localPath)
        remoteDirectory = posixpath.dirname(remotePath)
        self.print("Uploading {} => {}".format(localPath, remotePath))
        self.recursiveMkdir(remoteDirectory)
        with open(localPath, "rb") as fh:
            self.uploadFile(fh, remotePath)
    
    def downloadFileByPath(self, localPath):
        remotePath = self.getRemotePath(localPath)
        self.print("Downloading {} => {}".format(remotePath, localPath))
        os.makedirs(os.path.dirname(localPath), exist_ok=True)
        with open(localPath, "wb") as fh:
            self.downloadFile(fh, remotePath)
    
    def createDirectoryByPath(self, localPath):
        remotePath = self.getRemotePath(localPath)
        self.print("Creating directory {}".format(remotePath))
        self.recursiveMkdir(remotePath)

    def deleteDirectoryByPath(self, localPath):

        remotePath = self.getRemotePath(localPath)
        self.print("Deleting directory {}".format(remotePath))
        self.rmd(remotePath)
        self.cacheDirs.remove(remotePath)

    def deleteFileByPath(self, localPath):
        remotePath = self.getRemotePath(localPath)
        self.print("Deleting file {}".format(remotePath))
        self.delete(remotePath) 
        self.unregisterHash(remotePath)

    def recursiveMkdir(self, remotePath):
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

