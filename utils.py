import posixpath
import os
from os.path import relpath, join
import hashlib

def toPosixPath(filePath):
    if os.path.sep == posixpath.sep:
        return filePath
    _, filePath = os.path.splitdrive(filePath)
    pathPieces = filePath.split(os.path.sep)
    return posixpath.sep.join(pathPieces)

def recursiveLocalList(baseDir):
    localFiles = set()
    localDirectories = set()
    for root, dirs, files in os.walk(baseDir):
        for file in files:
            if relpath(root, baseDir) == '.':
                localFiles.add(file)
            else:
                relativePath = join(relpath(root, baseDir), file)
                localFiles.add(toPosixPath(relativePath))
        for directory in dirs:
            localDirectory = os.path.join(root, directory)
            localDirectories.add(localDirectory)
    return localFiles, localDirectories


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

def getFileMd5(filePath):
    fileMd5 = ""
    with open(filePath, "rb") as fh:
        fileMd5 = getMd5(fh)
    return fileMd5
