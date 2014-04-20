package org.vaadin.addons.lazycontainer

import scala.slick.driver.JdbcDriver
import scala.util.{Failure, Try, Success}
import scala.slick.lifted.CompiledFunction
import scala.ref.WeakReference
import java._
import scala.collection.JavaConversions

/**
 * Created by KangWoo,Lee on 14. 4. 20.
 */
abstract class SlickRetrieveDelegate[Drv <: JdbcDriver, I <: AnyRef, D](val drv: Drv) extends RetrieveDelegate[I, D] {

  import drv.simple._

  type BasicColumnParameterType = (ConstColumn[Long], ConstColumn[Long], Column[Option[Int]], Column[I])
  type BasicParameterType = (Long, Long, Option[Int], I)
  type ExtraColumnParameterType
  type ExtraParameterType
  type ColumnParameterType = (BasicColumnParameterType, ExtraColumnParameterType)
  type ParameterType = (BasicParameterType, ExtraParameterType)
  type TableType
  type DomainType
  type ConditionQuery = Query[TableType, DomainType, Seq]
  type RowNumberQuery = Query[(Column[Option[Int]], Column[I]), (Option[Int], I), Seq]
  type GetIdsQuery = Query[Column[I], I, Seq]
  type ListCompiledQuery = CompiledFunction[_, _, ParameterType, _, Seq[DomainType]]
  type SizeCompiledQuery = CompiledFunction[_, _, ParameterType, _, Int]
  type RowNumberCompiledQuery = CompiledFunction[_, _, ParameterType, _, Seq[(Option[Int], I)]]
  type GetIdsCompiledQuery = CompiledFunction[_, _, ParameterType, _, Seq[I]]

  // cached-compiled-query
  private var sizeCompiledQuery: Option[SizeCompiledQuery] = None
  private var listCompiledQuery: Option[ListCompiledQuery] = None
  private var indexOfIdCompiledQuery: Option[RowNumberCompiledQuery] = None
  private var getIdByIndexCompiledQuery: Option[RowNumberCompiledQuery] = None
  private var getIdsCompiledQuery: Option[GetIdsCompiledQuery] = None
  // there must be row_number window function in dbms.
  private val row_number = SimpleFunction.nullary[Option[Int]]("row_number")
  // for skip getIdByIndex
  private var enableGetIdByIndex: Try[Unit] = Success()
  // for auto cache-refresh
  private var columnsRef: WeakReference[Array[AnyRef]] = new WeakReference[Array[AnyRef]](null)
  private var ascendingRef: WeakReference[Array[Boolean]] = new WeakReference[Array[Boolean]](null)

  // util

  private def ret[A](t: Try[A]): A = t match {
    case Success(s) => s
    case Failure(e) => throw e
  }

  private def checkCacheClearingRequired(columns: Array[AnyRef], ascending: Array[Boolean]): Unit = {
    def comp(a: (Option[_], Option[_])): Boolean = a._1 != a._2
    // here, WeakReference is used for auto cache-refresh. Is this ok?
    val p1 = Option(columns).map(_.toList)
    val p2 = columnsRef.get.map(_.toList)
    val q1 = Option(ascending).map(_.toList)
    val q2 = ascendingRef.get.map(_.toList)
    ((p1, p2) ::(q1, q2) :: Nil).
      find(comp).
      map(_ => clearCompiledQuery()).
      map(_ => columnsRef = new WeakReference[Array[AnyRef]](columns)).
      map(_ => ascendingRef = new WeakReference[Array[Boolean]](ascending))
  }

  private def innerGetIdByIndex(index: Int, columns: Array[AnyRef], ascending: Array[Boolean], condition: util.Map[AnyRef, AnyRef]): AnyRef = {
    checkCacheClearingRequired(columns, ascending)
    if (getIdByIndexCompiledQuery.isEmpty)
      refreshGetIdByIndexQuery(columns, ascending)
    implicit val s = createSession()
    val r = Try {
      getIdByIndexCompiledQuery.
        map(_.apply((-1L, -1L, Option(index), nullId), prepareParameter(condition))).
        map(_.run)
    }
    s.close()
    ret(r).
      filter(!_.isEmpty).
      map(_.head._2).
      getOrElse(nullId)
  }

  // customize

  protected def prepareCondition(p: ColumnParameterType): ConditionQuery

  protected def prepareParameter(condition: util.Map[AnyRef, AnyRef]): ExtraParameterType

  protected def prepareOrdered(t: TableType, columns: Array[AnyRef], ascending: Array[Boolean]): slick.lifted.Ordered

  protected def compare(a: Column[I], b: Column[I]): Column[Option[Boolean]]

  protected def idColumn(t: TableType): Column[I]

  protected def nullId: I

  protected def modifier(x: DomainType): D

  protected def createSession(): Session

  // prepare

  private def prepareRowNumber(columns: Array[AnyRef], ascending: Array[Boolean])(p: ColumnParameterType) = {
    prepareCondition(p).map {
      case t ⇒ (row_number :: Over.orderBy(prepareOrdered(t, columns, ascending)), idColumn(t))
    }.asInstanceOf[RowNumberQuery]
  }

  protected def prepareIndexOfId(columns: Array[AnyRef], ascending: Array[Boolean])(p: ColumnParameterType): RowNumberQuery = {
    val ((_, _, _, idParam), _) = p
    prepareRowNumber(columns, ascending)(p).filter {
      case (row_num, id) ⇒ compare(id, idParam)
    }
  }

  protected def prepareGetIdByIndex(columns: Array[AnyRef], ascending: Array[Boolean])(p: ColumnParameterType): RowNumberQuery = {
    val ((_, _, idx, _), _) = p
    prepareRowNumber(columns, ascending)(p).filter {
      case (row_num, id) ⇒ row_num === idx
    }
  }

  protected def prepareSize(p: ColumnParameterType): Column[Int] = prepareCondition(p).length

  protected def prepareList(columns: Array[AnyRef], ascending: Array[Boolean])(p: ColumnParameterType): ConditionQuery = {
    prepareCondition(p).
      sortBy(prepareOrdered(_, columns, ascending)).
      drop(p._1._1).
      take(p._1._2)
  }

  protected def prepareGetIds(columns: Array[AnyRef], ascending: Array[Boolean])(p: ColumnParameterType): GetIdsQuery = {
    prepareCondition(p).
      sortBy(prepareOrdered(_, columns, ascending)).
      map(idColumn)
  }

  // compile

  protected def compileSize(): SizeCompiledQuery // = Compiled(prepareSize _)

  protected def compileList(columns: Array[AnyRef], ascending: Array[Boolean]): ListCompiledQuery // = Compiled(prepareList(columns, ascending) _)

  protected def compileIndexOfId(columns: Array[AnyRef], ascending: Array[Boolean]): RowNumberCompiledQuery // = Compiled(prepareIndexOfId(columns, ascending) _)

  protected def compileGetIdByIndex(columns: Array[AnyRef], ascending: Array[Boolean]): RowNumberCompiledQuery // = Compiled(prepareGetIdByIndex(columns, ascending) _)

  protected def compileGetIds(columns: Array[AnyRef], ascending: Array[Boolean]): GetIdsCompiledQuery // = Compiled(prepareGetIds(columns, ascending) _)

  // refresh

  protected def refreshListQuery(columns: Array[AnyRef], ascending: Array[Boolean]): Unit = {
    listCompiledQuery = Option[ListCompiledQuery](compileList(columns, ascending))
  }

  protected def refreshIndexOfIdQuery(columns: Array[AnyRef], ascending: Array[Boolean]): Unit = {
    indexOfIdCompiledQuery = Option[RowNumberCompiledQuery](compileIndexOfId(columns, ascending))
  }

  protected def refreshGetIdByIndexQuery(columns: Array[AnyRef], ascending: Array[Boolean]): Unit = {
    getIdByIndexCompiledQuery = Option[RowNumberCompiledQuery](compileGetIdByIndex(columns, ascending))
  }

  protected def refreshGetIdsQuery(columns: Array[AnyRef], ascending: Array[Boolean]): Unit = {
    getIdsCompiledQuery = Option[GetIdsCompiledQuery](compileGetIds(columns, ascending))
  }

  protected def refreshSizeQuery(): Unit = {
    sizeCompiledQuery = Option[SizeCompiledQuery](compileSize())
  }

  // public api

  def clearCompiledQuery(): Unit = {
    listCompiledQuery = None
    indexOfIdCompiledQuery = None
    getIdByIndexCompiledQuery = None
    getIdsCompiledQuery = None
  }

  def useGetIdByIndex(b: Boolean = true): Unit = {
    if (b)
      enableGetIdByIndex = Success()
    else
      enableGetIdByIndex = Failure(new IndexOutOfBoundsException)
  }

  override def indexOfId(itemId: I, columns: Array[AnyRef], ascending: Array[Boolean], condition: util.Map[AnyRef, AnyRef]): Int = {
    checkCacheClearingRequired(columns, ascending)
    if (indexOfIdCompiledQuery.isEmpty)
      refreshIndexOfIdQuery(columns, ascending)
    implicit val s = createSession()
    val r = Try {
      indexOfIdCompiledQuery.
        map(_.apply((-1L, -1L, None, itemId), prepareParameter(condition))).
        map(_.run)
    }
    s.close()
    ret(r).
      filter(!_.isEmpty).
      flatMap(_.head._1).
      getOrElse(-1)
  }

  override def getList(startIndex: Int, numberOfItems: Int, columns: Array[AnyRef], ascending: Array[Boolean], condition: util.Map[AnyRef, AnyRef]): util.List[D] = {
    checkCacheClearingRequired(columns, ascending)
    if (listCompiledQuery.isEmpty)
      refreshListQuery(columns, ascending)
    implicit val s = createSession()
    val r = Try {
      listCompiledQuery.
        map(_.apply((startIndex, numberOfItems, None, nullId), prepareParameter(condition))).
        map(_.run)
    }
    s.close()
    ret(r).
      map(_.map(modifier)).
      map(JavaConversions.seqAsJavaList).get
  }


  override def getIdByIndex(index: Int, columns: Array[AnyRef], ascending: Array[Boolean], condition: util.Map[AnyRef, AnyRef]): AnyRef = {
    /**
     * in sometimes, getIdByIndex can causes overhead.
     * so when sql-execution is not required, skip it.
     */
    enableGetIdByIndex.
      map(_ => innerGetIdByIndex(index, columns, ascending, condition)).
      get
  }

  override def getIds(columns: Array[AnyRef], ascending: Array[Boolean], condition: util.Map[AnyRef, AnyRef]): util.Collection[I] = {
    checkCacheClearingRequired(columns, ascending)
    if (getIdsCompiledQuery.isEmpty)
      refreshGetIdsQuery(columns, ascending)
    implicit val s = createSession()
    val r = Try {
      getIdsCompiledQuery.
        map(_.apply((-1L, -1L, None, nullId), prepareParameter(condition))).
        map(_.run)
    }
    s.close()
    ret(r).
      map(JavaConversions.seqAsJavaList).get
  }

  override def size(condition: util.Map[AnyRef, AnyRef]): Int = {
    if (sizeCompiledQuery.isEmpty)
      refreshSizeQuery()
    implicit val s = createSession()
    val r = Try {
      sizeCompiledQuery.
        map(_.apply((-1L, -1L, None, nullId), prepareParameter(condition))).
        map(_.run)
    }
    s.close()
    ret(r).get
  }
}
