package sharesSimulator

import scala.util.hashing.MurmurHash3

// TODO much shorter hash functions for ints: https://burtleburtle.net/bob/hash/integer.html
case class Hash(seed: Int, max: Int) {

  /**
    * Hashes i to a number between 0 and max (max exclusive)
    * @param i
    * @return
    */
  def hash(i: Int): Int = {
    Math.abs(MurmurHash3.finalizeHash(MurmurHash3.mix(seed, i), 1)) % max
  }

}
