package com.simagis.claims.web.ui

import com.simagis.claims.rest.api.ClaimDb
import com.simagis.edi.mdb.`+$set`
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.vaadin.annotations.Title
import com.vaadin.annotations.VaadinServletConfiguration
import com.vaadin.data.provider.QuerySortOrder
import com.vaadin.event.ShortcutAction
import com.vaadin.icons.VaadinIcons
import com.vaadin.server.Sizeable
import com.vaadin.server.VaadinRequest
import com.vaadin.server.VaadinServlet
import com.vaadin.shared.data.sort.SortDirection
import com.vaadin.ui.*
import com.vaadin.ui.renderers.DateRenderer
import com.vaadin.ui.renderers.HtmlRenderer
import com.vaadin.ui.themes.ValoTheme
import org.bson.Document
import java.text.SimpleDateFormat
import javax.servlet.annotation.WebServlet


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
@Title("Claim Queries")
class ClaimQueryExplorerUI : UI() {

    private val claimQueryPreview = ClaimQueryPreview()

    override fun init(request: VaadinRequest?) {
        fun Grid<ClaimQuery>.editItem(claimQuery: ClaimQuery) {
            val dialog = ClaimQueryEditor.Dialog.of(claimQuery)
            dialog.editor.onValueChanged += {
                refresh()
                select(it.value)
            }
            getCurrent().addWindow(dialog)
        }

        val claimQueryGrid = Grid<ClaimQuery>().apply {
            setSizeFull()
            setSelectionMode(Grid.SelectionMode.SINGLE)

            fun <V> addColumn(
                    caption: String,
                    isSortable: Boolean = false,
                    isHidden: Boolean = false,
                    isHidable: Boolean = false,
                    function: (ClaimQuery) -> V): Grid.Column<ClaimQuery, V> = addColumn(function)
                    .also {
                        it.caption = caption
                        it.isSortable = isSortable
                        it.isHidden = isHidden
                        it.isHidable = isHidable
                    }

            addColumn("Name", isSortable = true) { it.name }
            addColumn("Type", isSortable = true) { it.type }
            addColumn("Created", isHidable = true, isSortable = true) { it.created }.setRenderer(DateRenderer(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")))
            addColumn("Modified", isHidable = true, isHidden = true, isSortable = true) { it.modified }.setRenderer(DateRenderer(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")))
            addColumn("find", isHidable = true, isHidden = true) { it.find }
            addColumn("projection", isHidable = true, isHidden = true) { it.projection }
            addColumn("sort", isHidable = true, isHidden = true) { it.sort }
            addColumn("page size", isHidable = true, isHidden = true, isSortable = true) { it.pageSize }

            fun ClaimQuery.toHtml() = """<div style="background-color:white;margin:3px">
                &nbsp;<a target="_blank" href="$href">${VaadinIcons.EXTERNAL_LINK.html}</a>&nbsp;</div>"""

            with(addColumn({ it.toHtml() })) {
                setRenderer(HtmlRenderer())
                caption = "URL"
                isSortable = false
                styleGenerator = StyleGenerator { "" }
                maximumWidth = 72.0
            }

            addSelectionListener {
                claimQueryPreview.value = it.firstSelectedItem.orElseGet { null }
            }

            refresh()
        }

        val toolbar = HorizontalLayout()
        toolbar.addComponent(Button("New").apply {
            icon = VaadinIcons.FILE_ADD
            addStyleName(ValoTheme.BUTTON_BORDERLESS)
            addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
            addClickListener {
                claimQueryGrid.editItem(ClaimQuery())
            }
        })
        toolbar.addComponent(Button("Edit").apply {
            icon = VaadinIcons.EDIT
            addStyleName(ValoTheme.BUTTON_BORDERLESS)
            addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
            setClickShortcut(ShortcutAction.KeyCode.F4)
            addClickListener {
                claimQueryGrid.editItem(claimQueryGrid.selectedItems.first())
            }
            isEnabled = false
            claimQueryGrid.addSelectionListener {
                isEnabled = it.firstSelectedItem.isPresent
            }
        })
        toolbar.addComponent(Button("Rename").apply {
            icon = VaadinIcons.INPUT
            addStyleName(ValoTheme.BUTTON_BORDERLESS)
            addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
            setClickShortcut(ShortcutAction.KeyCode.F2)
            addClickListener {
                val current = claimQueryGrid.selectedItems.first()
                val nameField = TextField("Name", current.name).apply {
                    setSizeFull()
                    focus()
                }
                val nameEditor = VerticalLayout(nameField).apply {
                    setSizeFull()
                    setMargin(false)
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
                        with(claimQueryGrid) {
                            refresh()
                            select(current)
                        }
                    }
                    !exists
                }
            }
            isEnabled = false
            claimQueryGrid.addSelectionListener {
                isEnabled = it.firstSelectedItem.isPresent
            }
        })
        toolbar.addComponent(Button("Delete").apply {
            icon = VaadinIcons.FILE_REMOVE
            addStyleName(ValoTheme.BUTTON_BORDERLESS)
            addStyleName(ValoTheme.BUTTON_ICON_ALIGN_TOP)
            addClickListener {
                val claimQuery = claimQueryGrid.selectedItems.first()
                showConfirmationDialog(
                        "Delete Claim Query",
                        """Do you wish to delete "${claimQuery.name}"""") {
                    ClaimDb.cq.deleteOne(Document("_id", claimQuery._id))
                    claimQueryGrid.refresh()
                    true
                }
            }
            isEnabled = false
            claimQueryGrid.addSelectionListener {
                isEnabled = it.firstSelectedItem.isPresent
            }
        })


        val splitPanel = HorizontalSplitPanel(claimQueryGrid, claimQueryPreview).apply {
            setSizeFull()
            setSplitPosition(70f, Sizeable.Unit.PERCENTAGE)
        }

        content = VerticalLayout().apply {
            setSizeFull()
            setMargin(false)
            isSpacing = false
            addComponent(toolbar)
            addComponentsAndExpand(splitPanel)
        }
    }

    private fun Grid<ClaimQuery>.refresh() = setDataProvider(
            { _: List<QuerySortOrder>, offset, limit ->
                val sort = doc {
                    fun SortDirection.toInt(): Int = when (this) {
                        SortDirection.DESCENDING -> 1
                        SortDirection.ASCENDING -> -1
                    }
                    sortOrder.forEach {
                        when (it.sorted.caption) {
                            "Name" -> `+`("name", it.direction.toInt())
                            "Type" -> `+`("type", it.direction.toInt())
                            "Created" -> `+`("created", it.direction.toInt())
                            "Modified" -> `+`("modified", it.direction.toInt())
                            "page size" -> `+`("pageSize", it.direction.toInt())
                        }
                    }
                }
                ClaimDb.cq.find()
                        .sort(sort)
                        .skip(offset)
                        .limit(limit)
                        .toList()
                        .map { it.toClaimQuery() }
                        .stream()
            },
            {
                ClaimDb.cq.find().count()
            })

    @WebServlet(urlPatterns = arrayOf("/cq/*", "/VAADIN/*"), name = "CQEServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = ClaimQueryExplorerUI::class, productionMode = false)
    class MyUIServlet : VaadinServlet()
}

