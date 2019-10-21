package experiments

case class SeekCounters(all: Long,
                        next: Long,
                        noneMats: Long,
                        firstLevel: Long,
                        intersect: Long,
                        firstIntersect: Long,
                        resizes: Long,
                        maxSize: Long,
                        binarySearch: Long,
                        linearSearch: Long
                       )

object SeekCounters {
  var all = 0L
  var next = 0L
  var noneMats = 0L
  var firstLevel = 0L
  var intersect = 0L
  var firstIntersect = 0L

  var resizes = 0L
  var maxSize = 0L

  var binarySearch = 0L
  var linearSearch = 0L

  def reset(): Unit = {
    noneMats = 0
    firstIntersect = 0
    intersect = 0
    firstIntersect =0

    resizes = 0
    maxSize = 0

    binarySearch = 0
    linearSearch = 0
  }

  def toCaseClass: SeekCounters = {
    SeekCounters(all, next, noneMats,
      firstLevel, intersect, firstIntersect,
      resizes, maxSize,
      binarySearch, linearSearch
    )
  }
}
