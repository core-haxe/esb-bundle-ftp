package esb.bundles.core.ftp;

import esb.core.config.sections.EsbConfig;
import haxe.Json;
import sys.io.File;
import sys.FileSystem;
import promises.PromiseUtils;
import promises.Promise;
import haxe.io.Path;
import ftp.FtpError;
import ftp.FtpConnectionDetails;
import ftp.FtpClient;
import esb.core.IBundle;
import esb.core.IProducer;
import esb.logging.Logger;
import esb.common.Uri;
import esb.core.Bus.*;
import esb.core.bodies.RawBody;

using StringTools;

@:keep
class FtpProducer implements IProducer {
    private static var log:Logger = new Logger("esb.bundles.core.ftp.FtpProducer");

    public var bundle:IBundle;
    public function start(uri:Uri) {
        log.info('creating producer for ${uri.toString()}');
        log.info('looking for files in "${uri.fullPath}"');

        listRemoteFiles(uri);
    }

    private function listRemoteFiles(uri:Uri) {
        var start:Float = 0;
        var client = new FtpClient();
        var connectionDetails:FtpConnectionDetails = {
            host: uri.domain,
            port: 22,
            username: uri.param("username"),
            password: uri.param("password")
        }

        var path = uri.path;
        if (path != null && !path.startsWith("/")) {
            path = "/" + path;
        }
        var pollInterval = uri.paramInt("pollInterval", 5000);
        var pattern:String = uri.params.get("pattern");
        var extensionPattern:String = null;
        if (pattern != null && pattern.startsWith("*.")) {
            extensionPattern = pattern.substring(2);
            pattern = null;
        }

        client.connect(connectionDetails).then(_ -> {
            return client.list(path);
        }).then(files -> {
            var eligibleItems = [];
            for (item in files) {
                var use = (item.type == File);
                if (pattern != null || extensionPattern != null) {
                    var path = new Path(item.name);
                    if (extensionPattern != null) {
                        use = path.ext == extensionPattern;
                    }
                }

                if (use) {
                    eligibleItems.push(item);
                }
            }
            start = Sys.time();
            var promises = [];
            for (eligibleItem in eligibleItems) {
                var itemFullPath = Path.normalize(path + "/" + eligibleItem.name);
                promises.push(processItem.bind(uri, client, itemFullPath));
            }
            return PromiseUtils.runAll(promises);
        }).then(_ -> {
            var end = Sys.time();
            trace("-------------------------------------------------> ALL DONE IN: ", Math.round((end - start) * 1000) + " ms");
            client.disconnect();
            haxe.Timer.delay(listRemoteFiles.bind(uri), pollInterval);
            return null;
        }, (error:FtpError) -> {
            trace("error", error);
            haxe.Timer.delay(listRemoteFiles.bind(uri), pollInterval);
        });
    }

    private function processItem(uri:Uri, client:FtpClient, fullPath:String):Promise<Bool> {
        return new Promise((resolve, reject) -> {
            var cacheFilename:String = null;
            var contentCacheFilename:String = null;
            var cacheInfo:CacheInfo = null;

            client.get(fullPath).then(fileBytes -> {
                var cacheFolder = EsbConfig.get().path("ftp-cache");
                cacheFolder = Path.normalize(cacheFolder + "/" + uri.domain);
                if (!FileSystem.exists(cacheFolder)) {
                    FileSystem.createDirectory(cacheFolder);
                }

                var hash = haxe.crypto.Md5.make(fileBytes).toHex();
                cacheFilename = Path.normalize(cacheFolder + "/" + hash + ".cache");
                contentCacheFilename = Path.normalize(cacheFolder + "/" + hash + ".content.cache");
                File.saveBytes(contentCacheFilename, fileBytes);
                if (FileSystem.exists(cacheFilename)) {
                    return null;
                }

                var path = new Path(fullPath);
                var message = createMessage(RawBody);
                message.properties.set("file.name", path.file);
                message.properties.set("file.extension", path.ext);
                message.properties.set("file.hash", hash);
                message.body.fromBytes(fileBytes);

                var originalFilename = path.file;
                if (path.ext != null) {
                    originalFilename += "." + path.ext;
                }
                cacheInfo = {
                    originalFilename: originalFilename,
                    fullRemotePath: fullPath
                }

                return to(uri, message);
            }).then(resultMessage -> {
                if (resultMessage != null) {
                    File.saveContent(cacheFilename, Json.stringify(cacheInfo, null, "  "));
                }
                resolve(true);
            }, (error:FtpError) -> {
                reject(error);
            });
        });
    }
}

typedef CacheInfo = {
    originalFilename:String,
    fullRemotePath:String
}