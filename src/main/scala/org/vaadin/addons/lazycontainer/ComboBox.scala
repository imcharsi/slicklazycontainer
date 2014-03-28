package org.vaadin.addons.lazycontainer

import com.vaadin.ui.{ComboBox => VaadinComboBox}
import com.vaadin.data.util.BeanItem
import com.vaadin.data.Property.{ValueChangeListener, ValueChangeEvent}

/**
 * Created by i on 14. 2. 2.
 */
trait ComboBox
[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY],
CF <: ContainerFactory[ID, VERSION, ENTITY, _ <: Container[ID, VERSION, ENTITY]]] {
  this: VaadinComboBox {
    val factory: CF
  } =>

  abstract class ComboBoxValueChangeListener[E <: EntityIdVersion[ID, VERSION, E]] extends ValueChangeListener {
    override def valueChange(event: ValueChangeEvent): Unit = {
      val entity = ready(event.getProperty.getValue.asInstanceOf[Option[ID]])
      setValue(None)
      entity.id match {
        case Some(_) => {
          container.getCondition.put(conditionAxis, entity)
        }
        case None => {
          container.getCondition.remove(conditionAxis)
        }
      }
    }

    def ready(id: Option[ID]): E

    val conditionAxis: AnyRef
  }

  trait CaptionConverter {
    def convert(b: ENTITY): String
  }

  override def getItemCaption(itemId: AnyRef): String = {
    val step1 = for {
      step3 <- captionConverter
      step2 <- Option(container.getItem(itemId).asInstanceOf[BeanItem[ENTITY]])
    } yield step3.convert(step2.getBean)
    val result = step1 match {
      case Some(s) => {
        s
      }
      case None => {
        getItemCaption(itemId)
      }
    }
    result
  }

  val container = factory.allocContainer(true)
  val converter = new container.EntityConverter
  var captionConverter: Option[CaptionConverter] = None

  setContainerDataSource(container)
  setConverter(converter)
  setImmediate(true)
}
