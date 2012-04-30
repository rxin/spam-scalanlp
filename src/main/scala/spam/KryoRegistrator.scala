package wiki

import com.esotericsoftware.kryo._

class KryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.setRegistrationOptional(true)
  }
}

