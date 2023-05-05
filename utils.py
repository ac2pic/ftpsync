import posixpath
import os
from os.path import relpath, join
import hashlib

def recursiveLocalList(baseDir):
    localFiles = set()
    for root, dirs, files in os.walk(baseDir):
        for file in files:
            if relpath(root, baseDir) == '.':
                localFiles.add(file)
            else:
                relativePath = join(relpath(root, baseDir), file)
                if os.path.sep != posixpath.sep:
                    rawRelativePath = relativePath
                    rawRelativePathPieces = rawRelativePath.split(os.path.sep)
                    relativePath = posixpath.sep.join(rawRelativePathPieces)
                localFiles.add(relativePath) 
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
