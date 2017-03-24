package com.simagis.claims.web.ui

import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.simagis.claims.rest.api.ClaimDb
import com.simagis.edi.mdb._id
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.vaadin.data.Binder
import com.vaadin.data.ValidationResult
import com.vaadin.data.Validator
import com.vaadin.data.converter.StringToIntegerConverter
import com.vaadin.data.validator.IntegerRangeValidator
import com.vaadin.event.ShortcutAction
import com.vaadin.icons.VaadinIcons
import com.vaadin.server.ExternalResource
import com.vaadin.server.Page
import com.vaadin.server.Sizeable
import com.vaadin.ui.*
import com.vaadin.ui.themes.ValoTheme
import org.bson.Document
import java.util.*

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
class ClaimQueryEditor : VerticalLayout() {
    private val binder = Binder<ClaimQuery>(ClaimQuery::class.java)
    var value: ClaimQuery = ClaimQuery()
        get() {
            binder.writeBean(field)
            return field
        }
        set(value) {
            field = value
            binder.readBean(field)
            updateURL()
            onValueChanged.forEach { it(ValueChangedEvent(this, field)) }
        }
    val onValueChanged: MutableList<(ValueChangedEvent) -> Unit> = mutableListOf()
    data class ValueChangedEvent(val source: ClaimQueryEditor, val value: ClaimQuery)

    private val urlLink = Link().apply {
        icon = VaadinIcons.EXTERNAL_LINK
        targetName = "_blank"
    }

    private val urlText = TextField().apply {
        setWidth(100f, Sizeable.Unit.PERCENTAGE)
        addStyleName(ValoTheme.TEXTFIELD_SMALL)
        isReadOnly = true
    }

    init {
        val jsonValidator = Validator<String> { value, _ ->
            try {
                Document.parse(value)
                ValidationResult.ok()
            } catch(e: Exception) {
                ValidationResult.error("json error: ${e.message}")
            }
        }

        fun jsonTextArea(name: String): TextArea = TextArea(name).apply {
            setSizeFull()
            binder.forField(this)
                    .withValidator(jsonValidator)
                    .bind(name)
        }


        val toolbar = HorizontalLayout().apply {
            setMargin(false)
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
                            value = current
                        }

                        if (current._id == null) {
                            val nameField = TextField("Name", current.name).apply {
                                setSizeFull()
                                focus()
                            }
                            val override = CheckBox("Override").apply { isVisible = false }
                            val nameEditor = VerticalLayout(nameField, override).apply {
                                setSizeFull()
                                setMargin(false)
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
                            value = current
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

            addComponents(Button("Format").apply {
                icon = VaadinIcons.CURLY_BRACKETS
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                setClickShortcut(
                        ShortcutAction.KeyCode.N,
                        ShortcutAction.ModifierKey.CTRL,
                        ShortcutAction.ModifierKey.ALT)
                addClickListener {
                    if (binder.validate().isOk) {
                        val temp = ClaimQuery()
                        binder.writeBean(temp)
                        binder.readBean(temp.format())
                    }
                }
            })


            addComponents(Button("Build").apply {
                icon = VaadinIcons.PLAY
                addStyleName(ValoTheme.BUTTON_BORDERLESS_COLORED)
                addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
                addClickListener {
                    updateURL()
                    urlText.focus()
                    urlText.selectAll()
                }
            })

            addComponentsAndExpand(VerticalLayout().apply {
                setWidth(100f, Sizeable.Unit.PERCENTAGE)
                isSpacing = false
                setMargin(false)
                addComponent(urlLink)
                addComponent(urlText)
                setComponentAlignment(urlLink, Alignment.TOP_RIGHT)
            })
        }

        val description = RichTextArea().apply {
            setSizeFull()
            addStyleName(ValoTheme.TEXTAREA_SMALL)
            binder.forField(this)
                    .bind("description")
        }

        val query = VerticalLayout().apply {
            setSizeFull()
            addComponentsAndExpand(VerticalLayout().apply {
                setSizeFull()
                setMargin(false)
                addComponents(
                        jsonTextArea("find").apply { focus() },
                        jsonTextArea("projection"),
                        jsonTextArea("sort"))
            })
            addComponent(
                    TextField("page size").apply {
                        binder.forField(this)
                                .withConverter(StringToIntegerConverter("Must enter a number"))
                                .withValidator(IntegerRangeValidator("Out of range", 1, 100))
                                .bind("pageSize")
                    })
        }

        val tabSheet = TabSheet().apply {
            setSizeFull()
            addTab(query, "Query")
            addTab(description, "Description")
        }

        addComponent(toolbar)
        addComponentsAndExpand(tabSheet)

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

    class Dialog : Window() {
        val editor: ClaimQueryEditor = ClaimQueryEditor()

        companion object {
            fun of(claimQuery: ClaimQuery): Dialog = Dialog().apply {
                editor.value = claimQuery
            }
        }

        init {
            editor.onValueChanged += {
                caption = it.value.name
            }
            content = editor
            setWidth(40f, Sizeable.Unit.PERCENTAGE)
            setHeight(90f, Sizeable.Unit.PERCENTAGE)
            center()
        }

    }
}
