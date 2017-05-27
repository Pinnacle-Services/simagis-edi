package com.simagis.claims.web.ui

import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.simagis.claims.rest.api.ClaimDb
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.vaadin.data.Binder
import com.vaadin.data.ValidationResult
import com.vaadin.data.Validator
import com.vaadin.data.converter.StringToIntegerConverter
import com.vaadin.data.validator.IntegerRangeValidator
import com.vaadin.data.validator.StringLengthValidator
import com.vaadin.event.ShortcutAction
import com.vaadin.icons.VaadinIcons
import com.vaadin.server.Page
import com.vaadin.server.UserError
import com.vaadin.shared.ui.ContentMode
import com.vaadin.shared.ui.ValueChangeMode
import com.vaadin.ui.*
import com.vaadin.ui.themes.ValoTheme
import org.bson.Document
import java.net.URI
import java.net.URISyntaxException
import java.net.URLEncoder
import java.util.*

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
class ClaimQueryEditor(private val explorer: ClaimQueryExplorerUI) : VerticalLayout() {
    private val binder = Binder<ClaimQuery>(ClaimQuery::class.java)
    private val nameField = TextField()
    private val descriptionField = TextField()
    private val queryPanel = HorizontalSplitPanel()
    private var value_id: Any? = null
    private lateinit var find: TextArea

    var value: ClaimQuery = ClaimQuery()
        get() {
            binder.writeBean(field)
            return field
        }
        set(value) {
            field = value
            value_id = value._id
            binder.readBean(field)
            updateURL()
        }

    private val parametersLayout = HorizontalLayout().apply {
        margin = margins()
        isSpacing = true
        addStyleName(ValoTheme.LAYOUT_HORIZONTAL_WRAPPING)
    }

    private val urlLink = Label("", ContentMode.HTML).apply {
        description = "Open Query Link in new window"
    }

    init {
        margin = margins()
        val jsonValidator = Validator<String> { value, _ ->
            try {
                Document.parse(value)
                ValidationResult.ok()
            } catch(e: Exception) {
                ValidationResult.error("json error: ${e.message}")
            }
        }

        var jsonTextAreaCurrent: TextArea? = null
        val jsonTextAreaList = mutableListOf<TextArea>()
        fun jsonTextArea(name: String, compactable: Boolean = true): TextArea = TextArea(name).apply {
            jsonTextAreaList += this
            widthK1 = size100pc
            rows = 3
            binder.forField(this)
                    .withValidator(jsonValidator)
                    .bind(name)

            addFocusListener {
                jsonTextAreaCurrent = this
                (parent as? AbstractOrderedLayout)?.let { layout ->
                    jsonTextAreaList
                            .filter { compactable && it !== this && layout === it.parent }
                            .forEach {
                                it.setHeightUndefined()
                                layout.setExpandRatio(it, 0f)
                            }
                    heightK1 = size100pc
                    layout.setExpandRatio(this, 1f)
                }
            }
        }

        fun reload(current: ClaimQuery) {
            explorer.cqGrid.refresh()
            explorer.cqGrid.select(current)
            value = current
        }

        fun Binder.BindingBuilder<ClaimQuery, String>.withNameValidator(): Binder.BindingBuilder<ClaimQuery, String>
                = withValidator(StringLengthValidator("Invalid length of Name", 1, 2048))
                .withValidator { value, _ ->
                    when {
                        value.startsWith("/") -> try {
                            URI(value)
                            ValidationResult.ok()
                        } catch(e: URISyntaxException) {
                            ValidationResult.error("URL syntax error: " + e.reason)
                        }
                        else -> ValidationResult.ok()
                    }
                }

        fun save(saveAsMode: Boolean = false) {
            binder.validate().onError {
                it.showError("Unable to save invalid values")
                return
            }

            val current = value.copy()
            binder.writeBean(current)
            if (saveAsMode) {
                current._id = null
            }

            fun insert(override: Boolean) {
                if (override) {
                    val filter = doc { `+`("name", current.name) }
                    val document = current.toDocument()
                    ClaimDb.cq.findOneAndReplace(filter, document,
                            FindOneAndReplaceOptions()
                                    .upsert(true)
                                    .returnDocument(ReturnDocument.AFTER)).let {
                        current._id = it._id
                    }
                } else {
                    val document = current.toDocument()
                    ClaimDb.cq.insertOne(document)
                    current._id = document._id
                }
                reload(current)
            }

            if (current._id == null) {
                val nameBinder = Binder<ClaimQuery>(ClaimQuery::class.java)
                val nameField = TextField("Name", current.name).apply {
                    setSizeFull()
                    nameBinder.forField(this)
                            .withNameValidator()
                            .bind("name")
                    focus()
                }
                val override = CheckBox("Override").apply { isVisible = false }
                val nameEditor = VerticalLayout(nameField, override).apply {
                    setSizeFull()
                    margin = margins()
                }
                showConfirmationDialog(
                        "Saving Claim Query",
                        actionType = ConfirmationActionType.PRIMARY,
                        actionCaption = "Save",
                        body = nameEditor) {

                    val validate = nameBinder.validate()
                    if (validate.isOk) {
                        val new = ClaimQuery()
                        nameBinder.writeBean(new)
                        current.name = new.name
                        if (override.value) {
                            insert(true)
                            true
                        } else {
                            val exists = ClaimDb.cq.find(doc { `+`("name", new.name) })
                                    .first() != null
                            if (exists) {
                                showError(
                                        """Claim Query "${new.name}" already exists""",
                                        "Use the Override")
                                override.isVisible = true
                            } else {
                                insert(false)
                            }
                            !exists
                        }
                    } else {
                        validate.showError("Invalid Name value")
                        false
                    }
                }
            } else {
                current.modified = Date()
                ClaimDb.cq.replaceOne(doc(current._id), current.toDocument())
                reload(current)
            }
        }

        val toolbar = HorizontalLayout().apply {
            margin = margins()

            addComponent(Button("New").apply {
                icon = VaadinIcons.FILE_ADD
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                addClickListener {
                    value = ClaimQuery()
                    explorer.cqGrid.deselectAll()
                    find.focus()
                }
            })

            addComponents(Button("Save").apply {
                icon = VaadinIcons.SERVER
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                addClickListener {
                    save()
                }
            })

            addComponent(Button("Save as").apply {
                icon = VaadinIcons.COPY
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                addClickListener {
                    save(saveAsMode = true)
                }
            })

            addComponent(Button("Rename").apply {
                icon = VaadinIcons.INPUT
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                setClickShortcut(ShortcutAction.KeyCode.F2)
                addClickListener {
                    val current = explorer.cqGrid.selectedItems.first()
                    val nameBinder = Binder<ClaimQuery>(ClaimQuery::class.java)
                    val nameField = TextField("Name", current.name).apply {
                        setSizeFull()
                        nameBinder.forField(this)
                                .withNameValidator()
                                .bind("name")
                        focus()
                    }
                    val nameEditor = VerticalLayout(nameField).apply {
                        setSizeFull()
                        margin = margins()
                    }

                    showConfirmationDialog(
                            "Renaming Claim Query",
                            actionType = ConfirmationActionType.FRIENDLY,
                            actionCaption = "Rename",
                            body = nameEditor
                    )
                    {
                        val validate = nameBinder.validate()
                        if (validate.isOk) {
                            val new = ClaimQuery()
                            nameBinder.writeBean(new)
                            val exists = ClaimDb.cq.find(doc { `+`("name", new.name) })
                                    .first() != null
                            if (exists) {
                                showError("""Claim Query "${new.name}" already exists""")
                            } else {
                                current.name = new.name
                                ClaimDb.cq.updateOne(
                                        doc(current._id),
                                        doc { `+$set` { `+`("name", new.name) } })
                                reload(current)
                            }
                            !exists
                        } else {
                            validate.showError("Invalid Name value")
                            false
                        }
                    }
                }
                isEnabled = false
                explorer.cqGrid.addSelectionListener {
                    isEnabled = it.firstSelectedItem.isPresent
                }
            })

            addComponent(Button("Delete").apply {
                icon = VaadinIcons.FILE_REMOVE
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                addClickListener {
                    val claimQuery = explorer.cqGrid.selectedItems.first()
                    showConfirmationDialog(
                            "Delete Claim Query",
                            """Do you wish to delete "${claimQuery.name}"""") {
                        ClaimDb.cq.deleteOne(Document("_id", claimQuery._id))
                        explorer.cqGrid.refresh()
                        true
                    }
                }
                isEnabled = false
                explorer.cqGrid.addSelectionListener {
                    isEnabled = it.firstSelectedItem.isPresent
                }
            })
        }

        val descriptionLabel = Label("", ContentMode.HTML).apply {
            widthK1 = size100pc
        }

        with(nameField) {
            binder.forField(this)
                    .bind("name")
        }

        with(descriptionField) {
            addValueChangeListener {
                descriptionLabel.value = it.value
            }
            binder.forField(this)
                    .bind("description")
        }

        val content = VerticalLayout().apply {
            margin = margins()
            isSpacing = false

            fun sectionMenu(): MenuBar = MenuBar().apply {
                addStyleName(ValoTheme.MENUBAR_BORDERLESS)
                addStyleName(ValoTheme.MENUBAR_SMALL)
            }

            fun sectionCaption(caption: String, menu: Component? = null) = HorizontalLayout().apply {
                margin = margins()
                isSpacing = false
                defaultComponentAlignment = Alignment.MIDDLE_LEFT
                addComponent(Label("&nbsp;<b>$caption</b>", ContentMode.HTML))
                if (menu != null)
                    addComponent(menu)
            }

            addComponent(sectionCaption("Description", sectionMenu().apply {
                addItem("", null).addItem("Edit", VaadinIcons.EDIT, MenuBar.Command {
                            val richTextArea = RichTextArea().apply {
                                value = descriptionField.value
                                widthK1 = size100pc
                            }
                            showConfirmationDialog(
                                    "Description",
                                    body = richTextArea,
                                    actionType = ConfirmationActionType.PRIMARY,
                                    actionCaption = "Ok")
                            {
                                descriptionField.value = richTextArea.value
                                true
                            }
                })
            }))
            addComponent(VerticalLayout().apply {
                margin = margins(left = true)
                setSizeUndefined()
                addComponent(descriptionLabel)
            })

            addComponent(sectionCaption("Link Parameters"))
            addComponent(VerticalLayout().apply {
                margin = margins(left = true)
                isSpacing = false
                addComponent(parametersLayout)
            })

            addComponent(sectionCaption("Link"))
            addComponent(VerticalLayout().apply {
                margin = margins(left = true)
                isSpacing = false
                addComponent(urlLink)
                addComponent(Label())
            })

            val codeMenu = sectionMenu()
            addComponent(sectionCaption("Code", codeMenu))
            addComponentsAndExpand(VerticalLayout().apply {
                margin = margins()

                val sectionContent = VerticalLayout().apply {
                    margin = margins(left = true)
                    addComponentsAndExpand(VerticalLayout().apply {
                        margin = margins()
                        isSpacing = false
                        addComponentsAndExpand(queryPanel.apply {
                            firstComponent = VerticalLayout().apply {
                                setSizeFull()
                                margin = margins(right = true, bottom = true)
                                addComponents(
                                        jsonTextArea("find").apply { find = this; focus() },
                                        jsonTextArea("sort"))
                            }
                            secondComponent = VerticalLayout().apply {
                                setSizeFull()
                                margin = margins(left = true, right = true, bottom = true)
                                addComponentsAndExpand(
                                        jsonTextArea("projection", false).apply { heightK1 = size100pc })
                                addComponents(
                                        ComboBox<String>("EDI Document Type").apply {
                                            setItems("835", "835c", "837", "835a", "837a")
                                            isEmptySelectionAllowed = false
                                            isTextInputAllowed = false
                                            itemCaptionGenerator = ItemCaptionGenerator {
                                                if (it.endsWith("a")) "$it (archive)" else it
                                            }
                                            binder.forField(this)
                                                    .bind("type")

                                        },
                                        TextField("Page Size").apply {
                                            binder.forField(this)
                                                    .withValidator(StringLengthValidator("Must enter a number 1-100", 1, 3))
                                                    .withConverter(StringToIntegerConverter("Must enter a number"))
                                                    .withValidator(IntegerRangeValidator("Out of range", 1, 100))
                                                    .bind("pageSize")
                                        },
                                        TextField("URL Path").apply {
                                            widthK1 = size100pc
                                            binder.forField(this)
                                                    .bind("path")
                                        }
                                )
                            }
                        })
                    })
                }

                codeMenu.addItem("", null).apply {
                    val itemsOnEdit = mutableListOf<MenuBar.MenuItem>()
                    fun updateVisibility() {
                        val isCodeSectionOpen = componentCount > 0
                        itemsOnEdit.forEach {
                            it.isVisible = isCodeSectionOpen
                        }
                    }
                    addItem("Show") {
                        when (componentCount) {
                            0 -> addComponentsAndExpand(sectionContent)
                            else -> removeAllComponents()
                        }
                        it.text = when (componentCount) {
                            0 -> "Show"
                            else -> "Hide"
                        }
                        updateVisibility()
                    }
                    itemsOnEdit += addSeparator()
                    itemsOnEdit += addItem("Reformat Code", VaadinIcons.CURLY_BRACKETS, MenuBar.Command {
                        jsonTextAreaCurrent?.let {
                            it.value = it.value.toJsonFormatted()
                            it.focus()
                        }
                    })
                    updateVisibility()
                }
            })
        }

        addComponent(toolbar)
        addComponentsAndExpand(content)

        value = ClaimQuery()
        updateURL()
    }

    private fun updateURL() = with(ClaimQuery()) {
        val valid = binder.validate().isOk
        parametersLayout.removeAllComponents()
        urlLink.componentError = null
        urlLink.isEnabled = valid
        fun URI.toHtml() = """<a
            target="_blank"
            href="${toASCIIString()}"
            >${toString()}&nbsp;${VaadinIcons.EXTERNAL_LINK.html}</a>"""
        if (valid) {
            binder.writeBean(this)
            fun updateLink(href: String) {
                try {
                    val uri = URI(href)
                    urlLink.value = Page.getCurrent().location.resolve(uri).toHtml()
                } catch(e: URISyntaxException) {
                    urlLink.componentError = UserError("Invalid URL: ${e.reason}")
                    urlLink.isEnabled = false
                    urlLink.value = URI("#").toHtml()
                }
            }
            val href = when {
                path.isNotBlank() && value_id != null -> {
                    val parameters = toParameters()
                    fun toHRef() :String {
                        return  "/query/$path?" + parameters.joinToString(separator = "&") {
                            "${it.name}=${it.default?.let { URLEncoder.encode(it, "UTF-8") } ?: ""}"
                        }
                    }
                    parameters.forEach { parameter ->
                        TextField(parameter.name, parameter.default ?: "").apply {
                            parametersLayout.addComponent(VerticalLayout(this).apply {
                                margin = margins()
                                setSizeUndefined()
                            })
                            valueChangeMode = ValueChangeMode.EAGER
                            addValueChangeListener {
                                parameter.default = it.value
                                updateLink(toHRef())
                            }
                        }
                    }
                    toHRef()
                }
                else -> "/claim/$type/=${encode()}?ps=$pageSize"
            }
            updateLink(href)

        } else {
            urlLink.value = URI("#").toHtml()
        }
    }
}
