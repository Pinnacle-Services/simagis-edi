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
import com.vaadin.server.ExternalResource
import com.vaadin.server.Page
import com.vaadin.shared.ui.ContentMode
import com.vaadin.ui.*
import com.vaadin.ui.themes.ValoTheme
import org.bson.Document
import java.util.*

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
class ClaimQueryEditor(private val explorer: ClaimQueryExplorerUI) : VerticalLayout() {
    private val binder = Binder<ClaimQuery>(ClaimQuery::class.java)
    private val descriptionField = TextField()
    private val queryPanel = HorizontalSplitPanel()
    private lateinit var find: TextArea

    var value: ClaimQuery = ClaimQuery()
        get() {
            binder.writeBean(field)
            return field
        }
        set(value) {
            field = value
            binder.readBean(field)
            updateURL()
        }

    private val urlLink = Link().apply {
        icon = VaadinIcons.EXTERNAL_LINK
        description = "Open Query Link in new window"
        targetName = "_blank"
    }

    private val urlText = TextField().apply {
        addStyleName(ValoTheme.TEXTFIELD_BORDERLESS)
        isReadOnly = true
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
                    val validate = binder.validate()
                    if (validate.isOk) {
                        val current = value
                        binder.writeBean(current)

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
                            val nameField = TextField("Name", current.name).apply {
                                setSizeFull()
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
                                current.name = nameField.value
                                if (override.value) {
                                    insert(true)
                                    true
                                } else {
                                    val exists = ClaimDb.cq.find(doc { `+`("name", nameField.value) })
                                            .first() != null
                                    if (exists) {
                                        Notification.show(
                                                """Claim Query "${nameField.value}" already exists""",
                                                "Use the Override", Notification.Type.HUMANIZED_MESSAGE)
                                        override.isVisible = true
                                    } else {
                                        insert(false)
                                    }
                                    !exists
                                }
                            }
                        } else {
                            current.modified = Date()
                            ClaimDb.cq.replaceOne(doc(current._id), current.toDocument())
                            reload(current)
                        }
                        updateURL()
                    } else {
                        Notification.show(
                                "Unable to save invalid values",
                                validate.beanValidationErrors.map { it.errorMessage }.joinToString(),
                                Notification.Type.WARNING_MESSAGE)
                    }
                }
            })

            addComponent(Button("Rename").apply {
                icon = VaadinIcons.INPUT
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                setClickShortcut(ShortcutAction.KeyCode.F2)
                addClickListener {
                    val current = explorer.cqGrid.selectedItems.first()
                    val nameField = TextField("Name", current.name).apply {
                        setSizeFull()
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
                        val exists = ClaimDb.cq.find(doc { `+`("name", nameField.value) })
                                .first() != null
                        if (exists) {
                            Notification.show(
                                    """Claim Query "${nameField.value}" already exists""",
                                    Notification.Type.HUMANIZED_MESSAGE)
                        } else {
                            current.name = nameField.value
                            ClaimDb.cq.updateOne(doc(current._id), doc {
                                `+$set` { `+`("name", current.name) }
                            })
                            reload(current)
                        }
                        !exists
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

            fun sectionCaption(caption: String) = Label("&nbsp;<b>$caption</b>", ContentMode.HTML)

            fun verticalToolbar(init: VerticalLayout.() -> Unit) = VerticalLayout().apply {
                margin = margins()
                isSpacing = false
                setWidthUndefined()
                init()
            }


            addComponent(sectionCaption("Description"))
            addComponent(HorizontalLayout().apply {
                margin = margins(right = true)
                addComponent(verticalToolbar {
                    addComponent(Button().apply {
                        description = "Edit Description"
                        icon = VaadinIcons.EDIT
                        addStyleName(ValoTheme.BUTTON_LINK)
                        addClickListener {
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
                        }
                    })
                })
                addComponentsAndExpand(descriptionLabel)
            })

            addComponent(sectionCaption("Query"))

            addComponentsAndExpand(HorizontalLayout().apply {
                margin = margins()

                addComponent(verticalToolbar {
                    addComponents(Button().apply {
                        description = "Build Query URL"
                        icon = VaadinIcons.PLAY
                        addStyleName(ValoTheme.BUTTON_LINK)
                        setClickShortcut(ShortcutAction.KeyCode.F9)
                        addClickListener {
                            updateURL()
                            urlText.focus()
                            urlText.selectAll()
                        }
                    })
                    addComponents(Button().apply {
                        description = "Reformat Code"
                        icon = VaadinIcons.CURLY_BRACKETS
                        addStyleName(ValoTheme.BUTTON_LINK)
                        setClickShortcut(
                                ShortcutAction.KeyCode.N,
                                ShortcutAction.ModifierKey.CTRL,
                                ShortcutAction.ModifierKey.ALT)
                        addClickListener {
                            jsonTextAreaCurrent?.let {
                                it.value = it.value.toJsonFormatted()
                                it.focus()
                            }
                        }
                    })
                })

                addComponentsAndExpand(VerticalLayout().apply {
                    margin = margins()
                    isSpacing = false
                    addComponent(HorizontalLayout().apply {
                        margin = margins()
                        isSpacing = false
                        defaultComponentAlignment = Alignment.MIDDLE_LEFT
                        addComponent(urlLink)
                        addComponentsAndExpand(urlText)
                    })

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
                                        setItems("835", "837")
                                        isEmptySelectionAllowed = false
                                        isTextInputAllowed = false
                                        binder.forField(this)
                                                .bind("type")

                                    },
                                    TextField("Page Size").apply {
                                        binder.forField(this)
                                                .withValidator(StringLengthValidator("Must enter a number 1-100", 1, 3))
                                                .withConverter(StringToIntegerConverter("Must enter a number"))
                                                .withValidator(IntegerRangeValidator("Out of range", 1, 100))
                                                .bind("pageSize")
                                    })
                        }
                    })
                })
            }
            )
        }

        addComponent(toolbar)
        addComponentsAndExpand(content)

        value = ClaimQuery()
        updateURL()
    }

    private fun updateURL() = with(ClaimQuery()) {
        val valid = binder.validate().isOk
        urlLink.isEnabled = valid
        urlText.isEnabled = valid
        if (valid) {
            binder.writeBean(this)
            urlLink.resource = ExternalResource(href)
            urlText.value = Page.getCurrent().location.resolve(href).toASCIIString()
        } else {
            urlLink.resource = ExternalResource("#")
            urlText.value = ""
        }
    }
}
