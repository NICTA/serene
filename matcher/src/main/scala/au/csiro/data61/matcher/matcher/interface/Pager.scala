package au.csiro.data61.matcher.matcher.interface

import au.csiro.data61.matcher.data._

trait Pager {
    val pageSize = 20
    def getNext(): List[(Int,Attribute)]
    def hasNext(): Boolean
}

case class AttributesPager(val attributes: List[Attribute]) extends Pager {
    var cursor = 0

    override def getNext(): List[(Int,Attribute)] = {
        val endIdx = Math.min(cursor+pageSize, attributes.size)
        val batch = (cursor until endIdx) zip attributes.slice(cursor, endIdx)
        cursor = endIdx
        batch.toList
    }

    override def hasNext(): Boolean = {
        return (cursor < attributes.size)
    }
}