package com.tw.apps

import com.amazonaws.services.cloudwatch.model.StandardUnit

case class Probe(
                  name: String,
                  failed: Boolean,
                  value: Int,
                  unit: StandardUnit)

