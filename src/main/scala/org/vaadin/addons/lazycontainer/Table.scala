package org.vaadin.addons.lazycontainer

import com.vaadin.ui.{Table => VaadinTable}
import com.vaadin.data.Property.{ValueChangeEvent, ValueChangeListener}
import com.vaadin.ui.TabSheet.{SelectedTabChangeEvent, SelectedTabChangeListener}

/**
 * Created by i on 14. 2. 2.
 */
trait Table
[ID, VERSION, ENTITY <: EntityIdVersion[ID, VERSION, ENTITY],
CF <: ContainerFactory[ID, VERSION, ENTITY, _ <: Container[ID, VERSION, ENTITY]]] extends SelectedTabChangeListener {
  this: VaadinTable {
    val factory: CF
  } =>

  abstract class TableValueChangeListener[E <: EntityIdVersion[ID, VERSION, E]] extends ValueChangeListener {
    override def valueChange(event: ValueChangeEvent): Unit = {
      val entity = ready(event.getProperty.getValue.asInstanceOf[Option[ID]])
      setValue(null)
      entity.id match {
        case Some(_) => {
          container.getCondition.put(conditionAxis, entity)
        }
        case None => {
          container.getCondition.remove(conditionAxis)
        }
      }
      container.clear()
      refreshRowCache()
    }

    def ready(id: Option[ID]): E

    val conditionAxis: AnyRef
  }

  def fireValueChange(): Unit = fireValueChange(false)

  val container = factory.allocContainer()
  val converter = new container.EntityConverter

  setContainerDataSource(container)
  setConverter(converter)
  setPageLength(5)
  setSelectable(true)
  setImmediate(true)
  for {
    (n, c) <- (factory.primaryProperty ++ factory.additionalProperty)
  } setConverter(n, c)

  override def selectedTabChange(event: SelectedTabChangeEvent): Unit = {
    container.clear()
  }
}
