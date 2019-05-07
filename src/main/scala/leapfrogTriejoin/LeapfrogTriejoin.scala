package leapfrogTriejoin
import Predef.assert
import util.control.Breaks._
import Predef._


class LeapfrogTriejoin(trieIterators: Map[EdgeRelationship, TrieIterator], variableOrdering: Seq[String]) {

  val allVariables = trieIterators.keys.flatMap(
    e => e.variables).toSet

  val maxDepth = allVariables.size - 1

  val DONE: Int = maxDepth
  val DOWN_ACTION: Int = maxDepth - 1
  val NEXT_ACTION: Int = -1
  val UP_ACTION: Int = maxDepth + 1



  require(allVariables == variableOrdering.toSet,
    s"The set of all variables in the relationships needs to equal the variable ordering. All variables: $allVariables, variableOrdering: $variableOrdering"
  )

  require(trieIterators.keys
    .forall(r => {
      val relevantVars = variableOrdering.filter(v => r.variables.contains(v)).toList
      relevantVars == relevantVars.sortBy(v => r.variables.indexOf(v))
    }),
    "Variable ordering differs for some relationships."
  )

  val leapfrogJoins: Array[LeapfrogJoin] = variableOrdering
    .map(v =>
      new LeapfrogJoin(trieIterators
        .filter({ case (r, _) => r.variables.contains(v) }).values.toArray))
    .toArray

  val variable2TrieIterators: Array[Array[TrieIterator]] = variableOrdering
    .map( v =>
      trieIterators.filter( { case (r, _) => r.variables.contains(v)}).values.toArray
    ).toArray

  var depth = -1
  var bindings = Array.fill(allVariables.size)(-1)
  var atEnd = trieIterators.values.exists(i => i.atEnd)  // Assumes connected join?

  if (!atEnd) {
    moveToNextTuple()
  }

  def next(): Array[Int] = {
    if (atEnd) {
      throw new IllegalStateException("Cannot call next of LeapfrogTriejoin when already at end.")
    }
    val tuple = bindings.clone()
    moveToNextTuple()

    tuple
  }

  private def moveToNextTuple() = {
    var action: Int = NEXT_ACTION
    if (depth == -1) {  // TODO true only once can be moved out of moveToNextTuple
      action = DOWN_ACTION
    } else if (currentLeapfrogJoin.atEnd) {
      action = UP_ACTION
    }
    if (action == NEXT_ACTION) {
      action = nextAction()
    }
    // TODO unrolling
    // TODO factor out all variables
    // TODO use compiler optimizer


    while (action != DONE) {
      if (action <= DOWN_ACTION) {
        triejoinOpen()
        //        leapfrogDistinctNext()
        if (currentLeapfrogJoin.atEnd) {
          action = UP_ACTION
        } else {
          bindings(depth) = currentLeapfrogJoin.key
          action = depth
        }
      } else if (action >= UP_ACTION) {
        if (depth == 0) {
          action = DONE
          atEnd = true
        } else {
          triejoinUp()
          if (currentLeapfrogJoin.atEnd) {
            action = UP_ACTION
          } else {
            action = nextAction()
          }
        }
      }
    }
  }

  @inline
  private def nextAction(): Int = {
    currentLeapfrogJoin.leapfrogNext()
    //        leapfrogDistinctNext()
    if (currentLeapfrogJoin.atEnd) {
      UP_ACTION
    } else {
      bindings(depth) = currentLeapfrogJoin.key
      depth
    }

  }

  @inline
  private def leapfrogDistinctNext(): Unit = {
    while (!currentLeapfrogJoin.atEnd && bindingsContains(currentLeapfrogJoin.key)) {
      currentLeapfrogJoin.leapfrogNext()
    }
  }

  @inline
  private def bindingsContains(key: Int): Boolean = {
    var i = 0
    var contains = false
    while (i < bindings.length) {
      contains |= bindings(i) == key
      i += 1
    }
    contains
  }

  private def triejoinOpen() ={
    depth += 1

    whileForeach(variable2TrieIterators(depth), _.open())

    leapfrogJoins(depth).init()
  }

  private def triejoinUp() = {
    whileForeach(variable2TrieIterators(depth), _.up())

    bindings(depth) = -1
    depth -= 1
  }


  @inline  // Faster than Scala's foreach because it actually gets inlined
  private def whileForeach(ts : Array[TrieIterator], f: TrieIterator => Unit): Unit = {
    var i = 0
    while (i < ts.length) {
      f(ts(i))
      i += 1
    }
  }

  private def currentLeapfrogJoin: LeapfrogJoin = {
    leapfrogJoins(depth)
  }
}
