package org.vaadin.addons.lazycontainer

import com.vaadin.data.util.AbstractBeanContainer.BeanIdResolver
import com.vaadin.data.util.converter.Converter
import java.util.Locale
import com.vaadin.data.util.BeanItem
import scala.reflect.ClassTag
import com.vaadin.ui.TabSheet.{SelectedTabChangeEvent, SelectedTabChangeListener}
import org.vaadin.addons.lazycontainer.Container.{ConverterMap, Optimizer}

/**
 * Created by i on 14. 2. 2.
 */
class Container
[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY]]
(needAlwaysRefresh: Boolean = false)
(implicit idTag: ClassTag[Option[ID]],
 versionTag: ClassTag[Option[VERSION]],
 entityTag: ClassTag[ENTITY]) extends LazyContainer[Option[ID], ENTITY](entityTag.runtimeClass.asInstanceOf[Class[ENTITY]]) {
  val idClass: Class[Option[ID]] = idTag.runtimeClass.asInstanceOf[Class[Option[ID]]]
  val versionClass: Class[Option[VERSION]] = versionTag.runtimeClass.asInstanceOf[Class[Option[VERSION]]]
  val entityClass: Class[ENTITY] = entityTag.runtimeClass.asInstanceOf[Class[ENTITY]]

  def clear() = clearCachedSize()

  override def needRefreshCachedSize(): Boolean = {
    /**
     * ComboBox 에서는 cache 를 쓰면 안된다.
     */
    if (needAlwaysRefresh)
      true
    else
      super.needRefreshCachedSize()
  }

  override def size(): Int = {
    val step1 = optimizer.map(f => f())
    val step2 = step1.getOrElse(false)
    val result = step2 match {
      case true => 0
      case false => super.size()
    }
    result
  }

  override def indexOfId(itemId: AnyRef): Int = {
    val result = itemId.asInstanceOf[Option[ID]] match {
      case s@Some(_) => {
        super.indexOfId(s)
      }
      case _ => {
        0
      }
    }
    result
  }

  class Resolver extends BeanIdResolver[Option[ID], ENTITY] {
    override def getIdForBean(bean: ENTITY): Option[ID] = {
      bean.id
    }
  }

  class NoneConverter extends Converter[AnyRef, Option[ID]] {
    override def getPresentationType: Class[AnyRef] = classOf[AnyRef]

    override def getModelType: Class[Option[ID]] = idClass

    override def convertToPresentation(value: Option[ID], targetType: Class[_ <: AnyRef], locale: Locale): AnyRef = {
      val result = value match {
        case s@Some(_) => {
          s
        }
        case _ => {
          None
        }
      }
      result
    }

    override def convertToModel(value: AnyRef, targetType: Class[_ <: Option[ID]], locale: Locale): Option[ID] = {
      val result: Option[ID] = value.asInstanceOf[Option[ID]] match {
        case s@Some(_) => {
          s
        }
        case _ => {
          None
        }
      }
      result
    }
  }

  class EntityConverter extends Converter[AnyRef, ENTITY] {

    override def getPresentationType: Class[AnyRef] = {
      classOf[AnyRef]
    }

    override def getModelType: Class[ENTITY] = {
      entityClass
    }

    override def convertToPresentation(value: ENTITY, targetType: Class[_ <: AnyRef], locale: Locale): AnyRef = {
      val result = Option(value) match {
        case Some(s) => {
          s.id
        }
        case None => {
          None
        }
      }
      result
    }

    override def convertToModel(value: AnyRef, targetType: Class[_ <: ENTITY], locale: Locale): ENTITY = {
      val item = getItem(value).asInstanceOf[BeanItem[ENTITY]]
      val result = Option(item) match {
        case Some(s) => {
          s.getBean
        }
        case None => {
          null.asInstanceOf[ENTITY]
        }
      }
      result
    }
  }

  var optimizer: Option[Optimizer] = None

  setBeanIdResolver(new Resolver)
}

object Container {
  type Optimizer = Unit => Boolean
  type ConverterMap = Map[String, Converter[String, _]]
}

trait ContainerFactory[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY], C <: Container[ID, VERSION, ENTITY]] {
  def allocContainer(needAlwaysRefresh: Boolean = false): C

  val primaryProperty: ConverterMap
  val additionalProperty: ConverterMap
}

