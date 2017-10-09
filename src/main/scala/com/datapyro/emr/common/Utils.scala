package com.datapyro.emr.common

import java.security.MessageDigest

object Utils {

  def md5(text: String): String = MessageDigest.getInstance("MD5").digest(text.getBytes).map(0xFF & _)
    .map {
      "%02x".format(_)
    }.foldLeft("") {
    _ + _
  }

}
