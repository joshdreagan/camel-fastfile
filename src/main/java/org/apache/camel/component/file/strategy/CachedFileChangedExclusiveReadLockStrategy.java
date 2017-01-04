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
package org.apache.camel.component.file.strategy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.component.file.GenericFileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedFileChangedExclusiveReadLockStrategy extends FileChangedExclusiveReadLockStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(CachedFileChangedExclusiveReadLockStrategy.class);

  private static final String CACHE_NAME = "fileCache";

  private Cache<Path, BasicFileAttributes> fileCache;

  @Override
  public void prepareOnStartup(GenericFileOperations<File> operations, GenericFileEndpoint<File> endpoint) {

    if (getTimeout() <= 0) {
      // The default is supposed to be 10000L, but seems to be 0L for some reason.
      setTimeout(10000L);
    }

    CachingProvider cacheProvider = Caching.getCachingProvider();
    CacheManager cacheManager = cacheProvider.getCacheManager();
    MutableConfiguration<Path, BasicFileAttributes> cacheConfig = new MutableConfiguration<>();
    cacheConfig.setStoreByValue(false);
    cacheConfig.setTypes(Path.class, BasicFileAttributes.class);
    cacheConfig.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, getTimeout())));
    fileCache = cacheManager.createCache(CACHE_NAME, cacheConfig);

    LOG.debug("Configured the read lock strategy using a checkInterval of [{}] millis and a timeout of [{}] millis.", getCheckInterval(), getTimeout());
  }

  @Override
  public boolean acquireExclusiveReadLock(GenericFileOperations<File> operations, GenericFile<File> file, Exchange exchange) throws Exception {

    boolean exclusive = false;

    Path target = file.getFile().toPath();

    BasicFileAttributes newStats = Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    BasicFileAttributes oldStats = fileCache.get(target);

    if (oldStats != null) {
      if (newStats.size() >= getMinLength()
              && (getMinAge() <= 0 || (getMinAge() > 0 && (newStats.lastModifiedTime().toMillis() - newStats.creationTime().toMillis()) >= getMinAge()))
              && (oldStats.size() == newStats.size() && oldStats.lastModifiedTime().equals(newStats.lastModifiedTime()))
              && ((System.currentTimeMillis() - newStats.lastModifiedTime().toMillis()) >= getCheckInterval())) {
        LOG.debug("File [{}] seems to have stopped changing. Attempting to grab a read lock...", target);
        exclusive = true;
      } else {
        LOG.debug("File [{}] is still changing. Will check it again on the next poll.", target);
        fileCache.put(target, newStats);
        exclusive = false;
      }
    } else {
      LOG.debug("File [{}] is not yet known. Will add it to the cache and check again on the next poll.", target);
      fileCache.put(target, newStats);
      exclusive = false;
    }

    if (exclusive) {
      LOG.debug("Got an exclusive lock for file [{}].", target);
      fileCache.remove(target);
    }
    return exclusive;
  }
}
