package com.bigdata.datatypes


case class Crimes(
                   ID: Long,
                   Date: String,
                   Month: String,
                   Type: String,
                   IUCR: String,
                   Arrest: Boolean,
                   Domestic: Boolean,
                   District: String,
                   ComArea: String,
                   Latitude: Double,
                   Longtitude: Double
                 )

