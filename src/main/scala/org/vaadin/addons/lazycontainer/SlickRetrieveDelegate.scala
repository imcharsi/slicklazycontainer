package org.vaadin.addons.lazycontainer

import scala.slick.driver.JdbcDriver
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions
import scala.slick.lifted.{AppliedCompiledFunction, CompiledFunction}


/**
 * Created by i on 14. 1. 20.
 */
abstract class SlickRetrieveDelegate
[IDTYPE, BEANTYPE, TABLETYPES, ENTITYTYPES, DRIVER <: JdbcDriver]
(val driver: DRIVER) extends RetrieveDelegate[IDTYPE, BEANTYPE] {

  import driver.simple._

  /**
   * TODO
   * 한가지 문제가 있다.
   * compiled 에서는 order by 를 인자화할 수 없다.
   * 차선책으로 정렬조건을 바꿀때마다 compile 을 새로 하는 수밖에 없다.
   * 주어진 정렬조건이 앞의 정렬조건과 다르다는 것을 어떻게 알 수 있나.
   * 편리하고 확실하게 알 수 있는 방법은 없는 듯한데,
   * 사용자가 정렬조건을 바꿀때 명시적으로 다시 compile 하도록 하자.
   * 이를 위한 method 가 또 필요하다. 예를 들면 def compile() 와 같이 하면 되겠다.
   * 아니다. compile 되면 어떤 방식으로도 아무것도 바꿀수 없다.
   * 정렬조건도 마찬가지이다. 그래서 그냥 매번 새로 번역하도록 해야 한다.
   * 비용이 많이 들듯 하다.
   */
  type ListQuery = Query[TABLETYPES, ENTITYTYPES]
  type IdIdxQuery = Query[(Column[IDTYPE], Column[Long]), (IDTYPE, Long)]
  type IdQuery = Query[Column[IDTYPE], IDTYPE]
  type IdxQuery = Query[Column[Long], Long]
  type ConditionMap = java.util.Map[AnyRef, AnyRef]

  def compareId(col: Column[IDTYPE], id: IDTYPE): Column[Option[Boolean]]

  def id(t: TABLETYPES): Column[IDTYPE]

  def applyCondition(condition: ConditionMap): ListQuery

  def session(): Session

  def modifier(a: ENTITYTYPES): BEANTYPE

  def sortBy(t: TABLETYPES, columns: Array[AnyRef], ascending: Array[Boolean]): scala.slick.lifted.Ordered

  def idx2id(query: IdIdxQuery, index: Int): IdQuery =
    for {
      (id, idx) <- query if idx === index.toLong
    } yield id

  def id2idx(query: IdIdxQuery, i: IDTYPE): IdxQuery =
    for {
      (id, idx) <- query if compareId(id, i)
    } yield idx

  private val row_number = SimpleFunction.nullary[Long]("row_number")

  def numbering(query: ListQuery, columns: Array[AnyRef], ascending: Array[Boolean]): IdIdxQuery = {
    query.map {
      case t => (id(t), ((row_number :: Over.orderBy(sortBy(t, columns, ascending))) - 1l))
    }.asInstanceOf[IdIdxQuery]
  }

  def pickId(query: ListQuery): IdQuery =
    for {
      t <- query
    } yield id(t)

  def ret[T](t: Try[T]): T = {
    t match {
      case Success(s) => {
        return s
      }
      case Failure(e) => {
        throw e
      }
    }
  }

  override def size(condition: ConditionMap): Int = {
    implicit val session = this.session()
    import session.withTransaction
    val t = Try {
      withTransaction {
        applyCondition(condition).length.run
      }
    }
    session.close()
    ret(t)
  }

  override def getIds(columns: Array[AnyRef], ascending: Array[Boolean], condition: ConditionMap): java.util.Collection[IDTYPE] = {
    implicit val session = this.session()
    import session.withTransaction
    val t = Try {
      withTransaction {
        val r = pickId(applyCondition(condition).sortBy(sortBy(_, columns, ascending)))
        JavaConversions.seqAsJavaList(r.list())
      }
    }
    session.close()
    ret(t)
  }

  override def getList(startIndex: Int, numberOfItems: Int, columns: Array[AnyRef], ascending: Array[Boolean], condition: ConditionMap): java.util.List[BEANTYPE] = {
    implicit val session = this.session()
    import session.withTransaction
    val t = Try {
      withTransaction {
        val r = applyCondition(condition).sortBy(sortBy(_, columns, ascending))
        val r2 = r.drop(startIndex).take(numberOfItems).list()
        val r3 = r2.map(modifier)
        JavaConversions.seqAsJavaList(r3)
      }
    }
    session.close()
    ret(t)
  }

  override def getIdByIndex(index: Int, columns: Array[AnyRef], ascending: Array[Boolean], condition: ConditionMap): AnyRef = {
    implicit val session = this.session()
    import session.withTransaction
    val t = Try {
      withTransaction {
        val r = numbering(applyCondition(condition), columns, ascending)
        val r2 = idx2id(r, index)
        r2.first().asInstanceOf[AnyRef]
      }
    }
    session.close()
    ret(t)
  }

  override def indexOfId(itemId: IDTYPE, columns: Array[AnyRef], ascending: Array[Boolean], condition: ConditionMap): Int = {
    implicit val session = this.session()
    import session.withTransaction
    val t = Try {
      withTransaction {
        val r = numbering(applyCondition(condition), columns, ascending)
        val r2 = id2idx(r, itemId)
        r2.first().toInt
      }
    }
    session.close()
    ret(t)
  }
}
