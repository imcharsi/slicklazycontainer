package org.vaadin.addons.lazycontainer

import scala.reflect.ClassTag

/**
 * Created by i on 14. 3. 29.
 */
class SlickLazyContainer[IDTYPE, BEANTYPE](delegate: Option[RetrieveDelegate[IDTYPE, BEANTYPE]])(implicit t: ClassTag[BEANTYPE]) extends LazyContainer[IDTYPE, BEANTYPE](t.runtimeClass.asInstanceOf[Class[BEANTYPE]]) {
  for (d <- delegate)
    setRetrieveDelegate(d)
}
