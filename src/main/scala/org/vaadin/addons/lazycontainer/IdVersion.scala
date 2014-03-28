package org.vaadin.addons.lazycontainer

import scala.slick.driver.JdbcDriver

/**
 * Created by i on 14. 2. 5.
 */
trait EntityIdVersion[ID, VERSION, A <: EntityIdVersion[ID, VERSION, A]] {
  var id: Option[ID]
  var version: Option[VERSION]

  def versionTouch: A
}

trait TableIdVersion[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY], DRV <: JdbcDriver] {
  this: DRV#Table[ENTITY] {
    val driver: DRV
  } =>

  import driver.simple._

  def id: Column[Option[ID]]

  def version: Column[Option[VERSION]]
}


trait VersionUpdater[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY], DRV <: JdbcDriver, TABLE <: DRV#Table[ENTITY] with TableIdVersion[ID, VERSION, ENTITY, DRV]] {
  this: {
    val driver: DRV
  } =>

  import driver.simple._

//  val query: TableQuery[TABLE, ENTITY]
  val query: TableQuery[TABLE]
  val cmp: (ENTITY, TABLE) => Column[Option[Boolean]]

  def update(entity: ENTITY)(implicit s: Session) = {
    /**
     * 이와 같이 하면 update 를 pattern 화 할수 있는데, version 을 함수적으로 하는 방법까지는 모르겠다.
     * 단순히, 주어진 instance 의 version 을 바꾸는 것은 여기서 바로 할수 있지만,
     * a.copy(version=version+1) 와 같이 하는 방법은 모르겠다. 그래서 밖에서 함수를 붙여주는 방식으로 하면 중복을 그나마 줄일수 있다.
     * IdVersion trait 에서 versionTouch 를 정의해두면, 결국 외부에서 함수를 붙여주는것을 entity class 에 옮겨붙이는 것과 같게 된다.
     * 이렇게 Table object 의 trait 로 해도 된다.
     */
    val newVal = entity.versionTouch
    val result = query.filter(cmp(entity, _)).update(newVal)
    if (result != 1)
      throw new RuntimeException
  }

  def insert(a: ENTITY)(implicit s: Session): Option[ID] = {
    query.returning {
      query.map {
        t => t.id
      }
    }.insert(a)
  }
}