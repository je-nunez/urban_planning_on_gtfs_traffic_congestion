package src.main.scala.cache

import scala.collection.JavaConverters._
import scala.Some
import net.sf.ehcache.Element
import net.sf.ehcache.config.{Configuration, CacheConfiguration}
import net.sf.ehcache.{Cache, CacheManager}

import src.main.scala.logging.Logging._


// A free version modification, from
// https://vinaycn.wordpress.com/2013/10/22/ehcache-as-a-scala-map/

class StringCache {

  protected [this] val cacheArea = createEhCache

  def createEhCache = {
    //build the ehCacheManager
    val managerName = "cacheManager"
    val name = "ehCache"
    val maxElementsInMemory: Int = 50000
    val eternal = false
    val timeToIdle = 0
    val timeToLive = 0

    val managerConfiguration = new Configuration()
    val cacheManager = new CacheManager(managerConfiguration)

    //build the cache
    var cacheName = name
    while (cacheManager.cacheExists(cacheName)) {
      // cache already exists.
      cacheName += "_"
    }
    if (!name.equals(cacheName)) {
      logMsg(WARNING, s"Cache '$name' already exists: creating a new one with name '$cacheName'")
    }

    val cacheConfiguration = new CacheConfiguration()
      .name(cacheName)
      .maxElementsInMemory(maxElementsInMemory)
      .eternal(eternal)
      .timeToIdleSeconds(timeToIdle)
      .timeToLiveSeconds(timeToLive)

    val cache = new Cache(cacheConfiguration)
    cacheManager.addCache(cache)
    //return the cache from the cacheManager
    cacheManager.getCache(cacheName)
  }


  def getValue(key: String): String = {
    val value = cacheArea.get(key)
    if (value != null) value.getObjectValue.asInstanceOf[String] else null
  }


  def get(key: String): Option[String] = {
    val value = getValue(key)
    if (value != null) Some(value) else None
  }



  def remove(key: String) {
    cacheArea.remove(key)
  }


  def add(kv: (String, String)) {
    cacheArea.put(new Element(kv._1, kv._2))
  }


  def update(key: String, value: String) {
    val element = cacheArea.get(key)
    if (element == null) throw new NoSuchElementException()
    cacheArea.putIfAbsent(new Element(key, value))
  }


  def apply(key: String): String = {
    val element = cacheArea.get(key)
    if (element == null) throw new NoSuchElementException()
    getValue(key)
  }


  def empty() {
    cacheArea.removeAll
  }


  def size: Int = cacheArea.getSize

}

