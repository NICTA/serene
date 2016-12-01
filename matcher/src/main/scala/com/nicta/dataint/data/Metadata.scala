package com.nicta.dataint.data

case class Metadata(val name: String, val description: String) {
	override def toString(): String = {
		s"""{name: "$name", description: "$description"}"""
	}
}