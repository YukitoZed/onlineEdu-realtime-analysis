package com.random.bean

import java.lang

case class LearnModel(
                       userId: Int,
                       cwareid: Int,
                       videoId: Int,
                       chapterId: Int,
                       edutypeId: Int,
                       subjectId: Int,
                       sourceType: String,
                       speed: Int,
                       ts: lang.Long,
                       te: lang.Long,
                       ps: Int,
                       pe: Int
                     )
