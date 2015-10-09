package src.main.scala.cache

import scala.collection.JavaConverters._
import scala.Some
import net.sf.ehcache.Element
import net.sf.ehcache.config.{Configuration, CacheConfiguration}
import net.sf.ehcache.{Cache, CacheManager}

import src.main.scala.logging.Logging._


// A free version modification, from
// https://vinaycn.wordpress.com/2013/10/22/ehcache-as-a-scala-map/

class KeyValueCache[K, V](val cacheName: String,
                          val maxElementsInMemory: Int = 50000,
                          val eternal: Boolean = false,
                          val timeToIdle: Int = 0,
                          val timeToLive: Int = 0) {

  protected [this] val cacheArea = createEhCache

  def createEhCache = {
    //build the ehCacheManager
    val managerName = "cacheManager"

    val managerConfiguration = new Configuration()
    val cacheManager = CacheManager.create(managerConfiguration)
    // val cacheManager = new CacheManager(managerConfiguration)

    //build the cache
    val uniqueCacheName = new StringBuilder(cacheName)
    while (cacheManager.cacheExists(uniqueCacheName.toString)) {
      // cache name already exists.
      uniqueCacheName.append("_")
    }

    val newUniqueCacheName = uniqueCacheName.toString
    if (!cacheName.equals(newUniqueCacheName)) {
      logMsg(WARNING, "Cache '%s' already exists: renaming it as '%s'".
                      format(cacheName, newUniqueCacheName))
    }

    // cacheManager.setName(newUniqueCacheName + "_mgr")
    val cacheConfiguration = new CacheConfiguration()
      .name(newUniqueCacheName)
      .maxElementsInMemory(maxElementsInMemory)
      .eternal(eternal)
      .timeToIdleSeconds(timeToIdle)
      .timeToLiveSeconds(timeToLive)

    val cache = new Cache(cacheConfiguration)
    cacheManager.addCache(cache)
    //return the cache from the cacheManager
    cacheManager.getCache(newUniqueCacheName)
  }


  // A key can exist at one instant but be purged from the cache by a
  // concurrent thread just an instant later, so use this method "keyExists()"
  // with care
  def keyExists(key: K): Boolean =
    cacheArea.isKeyInCache(key)


  def getValue(key: K): V = {
    val value = cacheArea.get(key)
    if (value != null) value.getObjectValue.asInstanceOf[V] else null.asInstanceOf[V]
  }


  def get(key: K): Option[V] = {
    val value = getValue(key)
    if (value != null) Some(value) else None
  }


  def remove(key: K) {
    cacheArea.remove(key)
  }


  def add(kv: (K, V)) {
    cacheArea.put(new Element(kv._1, kv._2))
  }


  def update(key: K, value: V) {
    val element = cacheArea.get(key)
    if (element == null) throw new NoSuchElementException()
    cacheArea.putIfAbsent(new Element(key, value))
  }


  def apply(key: K): V = {
    val element = cacheArea.get(key)
    if (element == null) throw new NoSuchElementException()
    getValue(key)
  }


  def empty() {
    cacheArea.removeAll
  }


  def size: Int = cacheArea.getSize

}

