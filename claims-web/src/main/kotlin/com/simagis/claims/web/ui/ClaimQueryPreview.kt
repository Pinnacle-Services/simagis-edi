package com.simagis.claims.web.ui

import com.vaadin.server.Sizeable
import com.vaadin.shared.ui.ContentMode
import com.vaadin.shared.ui.MarginInfo
import com.vaadin.ui.Label
import com.vaadin.ui.TextArea
import com.vaadin.ui.TextField
import com.vaadin.ui.VerticalLayout
import com.vaadin.ui.themes.ValoTheme

class ClaimQueryPreview : VerticalLayout() {
    private val name = Label().apply { addStyleName(ValoTheme.LABEL_LARGE) }
    private val description = Label("", ContentMode.HTML).apply { setWidth(100f, Sizeable.Unit.PERCENTAGE) }
    private val find = jsonTextArea("find")
    private val projection = jsonTextArea("projection")
    private val sort = jsonTextArea("sort")
    private val pageSize = TextField("page Size").apply { isReadOnly = true }
    private val queryLayout = VerticalLayout(find, projection, sort, pageSize).apply {
        setSizeUndefined()
        setWidth(100f, Sizeable.Unit.PERCENTAGE)
        setMargin(false)
        isVisible = false
    }

    private fun jsonTextArea(name: String): TextArea = TextArea(name).apply {
        setSizeUndefined()
        setWidth(100f, Sizeable.Unit.PERCENTAGE)
        isReadOnly = true
    }

    var value: ClaimQuery? = ClaimQuery()
        get() = field
        set(value) {
            field = value
            if (value != null) {
                name.value = value.name
                description.value = value.description
                queryLayout.isVisible = true
                find.value = value.find
                projection.value = value.projection
                sort.value = value.sort
                pageSize.value = value.pageSize.toString()
            } else {
                name.value = ""
                description.value = ""
                queryLayout.isVisible = false
            }

        }

    init {
        setWidth(100f, Sizeable.Unit.PERCENTAGE)

        margin = MarginInfo(false, true, true, true)

        addComponent(name)
        addComponentsAndExpand(description)
        addComponent(queryLayout)
    }

}