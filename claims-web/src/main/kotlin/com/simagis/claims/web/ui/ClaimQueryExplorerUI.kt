package com.simagis.claims.web.ui

import com.vaadin.annotations.Title
import com.vaadin.annotations.VaadinServletConfiguration
import com.vaadin.server.Sizeable
import com.vaadin.server.VaadinRequest
import com.vaadin.server.VaadinServlet
import com.vaadin.ui.*
import com.vaadin.ui.renderers.DateRenderer
import com.vaadin.ui.themes.ValoTheme
import java.text.SimpleDateFormat
import javax.servlet.annotation.WebServlet


/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
@Title("Claim Queries")
class ClaimQueryExplorerUI : UI() {
    private lateinit var cqEditor: ClaimQueryEditor
    lateinit var cqGrid: Grid<ClaimQuery>

    override fun init(request: VaadinRequest?) {
        val splitPanel = HorizontalSplitPanel().apply {
            setSizeFull()
            setSplitPosition(30f, Sizeable.Unit.PERCENTAGE)
            addStyleName(ValoTheme.SPLITPANEL_LARGE)
        }

        cqGrid = Grid<ClaimQuery>().apply {
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
            addColumn("Path", isHidable = true, isHidden = true, isSortable = true) { it.path }
            addColumn("Type", isSortable = true) { it.type }.apply { maximumWidth = 96.0 }
            addColumn("Created", isHidable = true, isHidden = true, isSortable = true) { it.created }.setRenderer(DateRenderer(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")))
            addColumn("Modified", isHidable = true, isHidden = true, isSortable = true) { it.modified }.setRenderer(DateRenderer(SimpleDateFormat("yyyy-MM-dd HH:mm:ss")))
            addColumn("find", isHidable = true, isHidden = true) { it.find }
            addColumn("projection", isHidable = true, isHidden = true) { it.projection }
            addColumn("sort", isHidable = true, isHidden = true) { it.sort }
            addColumn("page size", isHidable = true, isHidden = true, isSortable = true) { it.pageSize }

            addSelectionListener {
                cqEditor.value = it.firstSelectedItem.orElseGet { ClaimQuery() }
            }
        }

        cqEditor = ClaimQueryEditor(this)

        splitPanel.firstComponent = VerticalLayout().apply {
            margin = margins()
            isSpacing = false
            addComponent(HorizontalLayout().apply {
                margin = margins(left = true)
                addComponent(Label("Claim Queries").apply { addStyleName(ValoTheme.LABEL_H2) })
            })
            addComponentsAndExpand(cqGrid)
        }
        splitPanel.secondComponent = cqEditor

        content = splitPanel

        cqGrid.refresh()
    }

    @WebServlet(urlPatterns = ["/cq/*", "/VAADIN/*"], name = "CQEServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = ClaimQueryExplorerUI::class, productionMode = false)
    class MyUIServlet : VaadinServlet()
}
