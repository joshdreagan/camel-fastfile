/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.file.remote.strategy;

import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.component.file.GenericFileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import org.apache.commons.net.ftp.FTPFile;

public class CachedFtpChangedExclusiveReadLockStrategy extends FtpChangedExclusiveReadLockStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(CachedFtpChangedExclusiveReadLockStrategy.class);

  private static final String CACHE_NAME = "ftpFileCache";

  private Cache<String, FTPFile> fileCache;

  @Override
  public void prepareOnStartup(GenericFileOperations<FTPFile> tGenericFileOperations, GenericFileEndpoint<FTPFile> tGenericFileEndpoint) throws Exception {

    if (getTimeout() <= 0) {
      // The default is supposed to be 20000L, but seems to be 0L for some reason.
      setTimeout(20000L);
    }

    CachingProvider cacheProvider = Caching.getCachingProvider();
    CacheManager cacheManager = cacheProvider.getCacheManager();
    MutableConfiguration<String, FTPFile> cacheConfig = new MutableConfiguration<>();
    cacheConfig.setStoreByValue(false);
    cacheConfig.setTypes(String.class, FTPFile.class);
    cacheConfig.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, getTimeout())));
    fileCache = cacheManager.createCache(CACHE_NAME, cacheConfig);

    LOG.debug("Configured the read lock strategy using a checkInterval of [{}] millis and a timeout of [{}] millis.", getCheckInterval(), getTimeout());
  }

  @Override
  public boolean acquireExclusiveReadLock(GenericFileOperations<FTPFile> operations, GenericFile<FTPFile> file, Exchange exchange) throws Exception {

    boolean exclusive = false;

    FTPFile target = file.getFile();

    FTPFile newStats = target;
    FTPFile oldStats = fileCache.get(target.getName());

    if (oldStats != null) {
      // There is no "created time" available when using FTP. So we can't check the minAge.
      if (newStats.getSize() >= getMinLength()
              && (oldStats.getSize() == newStats.getSize() && oldStats.getTimestamp().equals(newStats.getTimestamp()))
              && ((System.currentTimeMillis() - newStats.getTimestamp().getTimeInMillis()) >= getCheckInterval())) {
        LOG.debug("File [{}] seems to have stopped changing. Attempting to grab a read lock...", target);
        exclusive = true;
      } else {
        LOG.debug("File [{}] is still changing. Will check it again on the next poll.", target);
        fileCache.put(target.getName(), newStats);
        exclusive = false;
      }
    } else {
      LOG.debug("File [{}] is not yet known. Will add it to the cache and check again on the next poll.", target);
      fileCache.put(target.getName(), newStats);
      exclusive = false;
    }

    if (exclusive) {
      LOG.debug("Got an exclusive lock for file [{}].", target);
      fileCache.remove(target.getName());
    }
    return exclusive;
  }
}
