package com.simagis.claims.web.ui

import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.rest.api.toJsonObject
import com.simagis.claims.rest.api.toStringPP
import com.vaadin.annotations.Title
import com.vaadin.annotations.VaadinServletConfiguration
import com.vaadin.data.Binder
import com.vaadin.data.ValidationResult
import com.vaadin.data.Validator
import com.vaadin.data.converter.StringToIntegerConverter
import com.vaadin.data.validator.IntegerRangeValidator
import com.vaadin.event.ShortcutAction
import com.vaadin.icons.VaadinIcons
import com.vaadin.server.ExternalResource
import com.vaadin.server.Sizeable
import com.vaadin.server.VaadinRequest
import com.vaadin.server.VaadinServlet
import com.vaadin.ui.*
import com.vaadin.ui.components.grid.MultiSelectionModelImpl
import com.vaadin.ui.renderers.ButtonRenderer
import com.vaadin.ui.renderers.DateRenderer
import com.vaadin.ui.themes.ValoTheme
import org.bson.Document
import java.text.SimpleDateFormat
import java.util.*
import javax.json.Json
import javax.json.JsonArray
import javax.servlet.annotation.WebServlet


/**
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/18/2017.
 */
@Title("Claim Query Builder")
class ClaimQueryBuilderUI : UI() {
    override fun init(vaadinRequest: VaadinRequest) {
        val jsonValidator = Validator<String> { value, _ ->
            try {
                Json.createReader(value.reader()).readObject()
                ValidationResult.ok()
            } catch(e: Exception) {
                ValidationResult.error("json error: ${e.message}")
            }
        }

        val validatorOnErroMessage = "Invalid fields"
        val binder = Binder<ClaimQuery>(ClaimQuery::class.java)
        fun Binder<ClaimQuery>.validate(onErrorMessage: String): Boolean = validate().isOk.also {
            if (!it) Notification.show(onErrorMessage, Notification.Type.WARNING_MESSAGE)
        }

        val splitPanel = HorizontalSplitPanel().apply {
            content = this
            setSizeFull()
            addStyleName(ValoTheme.SPLITPANEL_LARGE)
        }

        val favoritesGrid = Grid<ClaimQuery>().apply {
            setSizeFull()
            setSelectionMode(Grid.SelectionMode.MULTI)
            (selectionModel as? MultiSelectionModelImpl<ClaimQuery>)?.apply {
                selectAllCheckBoxVisibility = MultiSelectionModelImpl.SelectAllCheckBoxVisibility.VISIBLE
            }


            addColumn({ VaadinIcons.ARROW_CIRCLE_LEFT.html }, ButtonRenderer<ClaimQuery>({
                binder.readBean(it.item.format())
            }).apply {
                isHtmlContentAllowed = true
            }).apply {
                isSortable = false
                frozenColumnCount++
            }

            fun <V> Grid<ClaimQuery>.addColumn(
                    caption: String,
                    isSortable: Boolean = false,
                    isHidden: Boolean = false,
                    isHidable: Boolean = false,
                    function: (ClaimQuery) -> V): Grid.Column<ClaimQuery, V> {
                return addColumn(function).also {
                    it.caption = caption
                    it.isSortable = isSortable
                    it.isHidden = isHidden
                    it.isHidable = isHidable
                }
            }

            addColumn("Date") { it.date }.setRenderer(DateRenderer(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")))
            addColumn("type") { it.type }
            addColumn("find") { it.find }
            addColumn("projection", isHidable = true) { it.projection }
            addColumn("sort", isHidable = true) { it.sort }
            addColumn("page size", isHidable = true) { it.pageSize }

            refresh()
        }

        val favoritesButtons = HorizontalLayout().apply {
            setMargin(false)
            addComponent(Button().apply {
                icon = VaadinIcons.ARROW_CIRCLE_RIGHT
                description = "Add to favorites"
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addClickListener {
                    if (binder.validate(validatorOnErroMessage)) {
                        val query = ClaimQuery().apply { binder.writeBean(this) }
                        ClaimDb.cqb.insertOne(query.toDocument())
                        favoritesGrid.refresh()
                    }
                }
            })
            addComponent(Button().apply {
                icon = VaadinIcons.FILE_REMOVE
                description = "Delete selected"
                addStyleName(ValoTheme.BUTTON_BORDERLESS)
                addClickListener {
                    Window("Delete Favorites").also { dialog ->
                        dialog.content = VerticalLayout().also { content ->
                            Label().apply {
                                content.addComponent(this)
                                styleName = ValoTheme.LABEL_LARGE
                                value = favoritesGrid.selectedItems.size.let {
                                    when (it) {
                                        1 -> "Do you wish to delete selected item"
                                        else -> "Do you wish to delete $it selected items"
                                    }
                                }
                            }
                            HorizontalLayout().also { buttons ->
                                content.addComponent(buttons)
                                content.setComponentAlignment(buttons, Alignment.BOTTOM_RIGHT)
                                Button("Delete").apply {
                                    buttons.addComponent(this)
                                    addStyleName(ValoTheme.BUTTON_DANGER)
                                    addClickListener {
                                        favoritesGrid.selectedItems.forEach {
                                            ClaimDb.cqb.deleteOne(Document("_id", it._id))
                                        }
                                        favoritesGrid.refresh()
                                        dialog.close()
                                    }
                                }
                                Button("Cancel").apply {
                                    buttons.addComponent(this)
                                    addClickListener { dialog.close() }
                                }
                            }
                        }
                        dialog.isResizable = false
                        dialog.isModal = true
                        dialog.setWidth(50f, Sizeable.Unit.PERCENTAGE)
                        ui.addWindow(dialog)
                    }
                }
                isEnabled = false
                favoritesGrid.addSelectionListener {
                    isEnabled = !it.allSelectedItems.isEmpty()
                }
            })
        }

        splitPanel.secondComponent = Panel("Favorites").apply {
            setSizeFull()
            addStyleName(ValoTheme.PANEL_BORDERLESS)
            content = VerticalLayout().apply {
                setSizeFull()
                setMargin(false)
                isSpacing = false
                addComponent(favoritesButtons)
                addComponentsAndExpand(favoritesGrid)
            }
        }

        VerticalLayout().apply {
            splitPanel.firstComponent = Panel("Claim Query Builder", this).apply {
                setSizeFull()
                addStyleName(ValoTheme.PANEL_BORDERLESS)
            }
            setSizeUndefined()
        }.also { form ->

            val jsonTextAreaList = mutableListOf<TextArea>()
            fun jsonTextArea(name: String): TextArea = TextArea(name).apply {
                form.addComponent(this)
                jsonTextAreaList += this
                setWidth(32f, Sizeable.Unit.EM)
                rows = 5

                binder.forField(this)
                        .withValidator(jsonValidator)
                        .bind(name)
                addFocusListener {
                    jsonTextAreaList.forEach { it.rows = 5 }
                    rows = 20
                }
            }

            ComboBox<String>("EDI Document Type").apply {
                form.addComponent(this)
                setItems("835", "837")
                isEmptySelectionAllowed = false
                isTextInputAllowed = false
                binder.forField(this)
                        .bind("type")
            }
            jsonTextArea("find")
            jsonTextArea("projection")
            jsonTextArea("sort")
            TextField("page size").apply {
                form.addComponent(this)
                binder.forField(this)
                        .withConverter(StringToIntegerConverter("Must enter a number"))
                        .withValidator(IntegerRangeValidator("Out of range", 1, 100))
                        .bind("pageSize")
            }

            binder.readBean(ClaimQuery().format())

            HorizontalLayout().also { footer ->
                form.addComponent(footer)

                Button("Build URL").apply {
                    footer.addComponent(this)
                    addStyleName(ValoTheme.BUTTON_PRIMARY)
                    setClickShortcut(ShortcutAction.KeyCode.ENTER, ShortcutAction.ModifierKey.CTRL)

                    var link: Link? = null
                    val popupView = PopupView(object : PopupView.Content {
                        override fun getPopupComponent(): Component = link ?: Label("?")
                        override fun getMinimizedValueAsHTML(): String = ""
                    }).apply {
                        footer.addComponent(this)
                        isHideOnMouseOut = false
                    }

                    addClickListener {
                        ClaimQuery().apply {
                            if (binder.validate(validatorOnErroMessage)) {
                                binder.writeBean(this)
                                format()
                                binder.readBean(this)

                                link = Link().apply {
                                    val href = "/claim/$type/=${encode()}?ps=$pageSize"
                                    caption = href
                                    resource = ExternalResource(href)
                                    targetName = "_blank"
                                }
                                popupView.isPopupVisible = true
                            }
                        }
                    }
                }
            }
        }
    }

    private fun Grid<ClaimQuery>.refresh() = setDataProvider(
            { _, offset, limit ->
                ClaimDb.cqb.find()
                        .sort(Document("date", -1))
                        .skip(offset)
                        .limit(limit)
                        .toList()
                        .map { it.toClaimQuery() }
                        .stream()
            },
            {
                ClaimDb.cqb.find().count()
            })

    data class ClaimQuery(
            var _id: Any? = null,
            var type: String = "835",
            var find: String = "{}",
            var projection: String = "{}",
            var sort: String = "{}",
            var pageSize: Int = 20,
            var date: Date = Date()
    ) {
        companion object {
            val encoder: Base64.Encoder = Base64.getUrlEncoder()
        }

        fun toJsonArray(): JsonArray = Json.createArrayBuilder().also { array ->
            array.add(find.toJsonObject())
            array.add(projection.toJsonObject())
            array.add(sort.toJsonObject())
        }.build()

        fun encode(): String = encoder.encodeToString(toJsonArray().toString().toByteArray())

        fun format(): ClaimQuery {
            find = find.toJsonObject().toStringPP()
            projection = projection.toJsonObject().toStringPP()
            sort = sort.toJsonObject().toStringPP()
            return this
        }

        fun toDocument(): Document = Document().apply {
            if (_id != null) append("_id", _id)
            append("type", type)
            append("find", find.toJsonObject().toString())
            append("projection", projection.toJsonObject().toString())
            append("sort", sort.toJsonObject().toString())
            append("pageSize", pageSize)
            append("date", date)
        }
    }

    private fun Document.toClaimQuery(): ClaimQuery = ClaimQuery(
            _id = get("_id"),
            type = getString("type") ?: "Invalid",
            find = getString("find") ?: "{}",
            projection = getString("projection") ?: "{}",
            sort = getString("sort") ?: "{}",
            pageSize = getInteger("pageSize") ?: 20,
            date = getDate("date") ?: Date()
    )

    @WebServlet(urlPatterns = arrayOf("/cqb/*"), name = "CQBServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = ClaimQueryBuilderUI::class, productionMode = false)
    class MyUIServlet : VaadinServlet()
}
