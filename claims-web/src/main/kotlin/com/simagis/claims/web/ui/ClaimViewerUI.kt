package com.simagis.claims.web.ui

import com.simagis.claims.clientName
import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.rest.api.toJsonObject
import com.simagis.claims.web.codes
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import com.vaadin.annotations.Title
import com.vaadin.annotations.VaadinServletConfiguration
import com.vaadin.event.ShortcutAction
import com.vaadin.server.*
import com.vaadin.ui.*
import com.vaadin.ui.themes.ValoTheme
import java.net.URI
import java.net.URLEncoder
import javax.json.JsonArray
import javax.json.JsonObject
import javax.servlet.annotation.WebServlet


private const val claimViewerTitle = "Claim Viewer"

@Title(claimViewerTitle)
class ClaimViewerUI : UI() {

    private val filter = ClaimsFilter(this)

    override fun init(request: VaadinRequest) {
        content = HorizontalSplitPanel().apply {
            firstComponent = filter
            secondComponent = filter.iFrame.apply { setSizeFull() }
            setSplitPosition(20f, Sizeable.Unit.PERCENTAGE, false)
        }

        filter.open(page.uriFragment)
        page.addPopStateListener { filter.open(page.uriFragment) }
    }

    class ClaimsFilter(
        private val ui: UI,
        val iFrame: BrowserFrame = BrowserFrame()
    ) : VerticalLayout() {
        class CodeItem(val code: String, val name: String) {
            override fun toString(): String = "$code | $name"
        }

        interface Filter {
            val name: String
            val component: Component
            val value: String?
        }

        class TextFilter(override val name: String, caption: String) : Filter {
            private val textField = TextField(caption)
                .apply { setWidth("100%") }
            override val component: Component get() = textField
            override val value: String?
                get() = textField.value
                    .let { if (it.isBlank()) null else it }
        }

        class CodesFilter(override val name: String, codesName: String, caption: String) : Filter {
            private val comboBox = ComboBox<CodeItem>(
                caption,
                codes[codesName]?.map { CodeItem(it.key, it.value) })
                .apply { setWidth("100%") }
            override val component: Component get() = comboBox
            override val value: String? get() = comboBox.value?.code
        }

        private val captionLabel = Label(claimViewerTitle)
        private val content = VerticalLayout()
        private var title: String = claimViewerTitle
            set(value)  {
                field = value
                captionLabel.value = value
                ui.page.setTitle(value)
            }

        init {
            margin = Margins.OFF
            addComponent(HorizontalLayout().apply {
                setWidth("100%")
                addStyleName(ValoTheme.WINDOW_TOP_TOOLBAR)
                addComponent(captionLabel)
            })
            addComponent(content)
        }

        fun open(path: String) {
            iFrame.source = ExternalResource("about:blank")
            title = claimViewerTitle
            content.removeAllComponents()

            val query = ClaimDb.cq
                .find(doc { `+`("path", path) })
                .firstOrNull()
                ?.toClaimQuery() ?: return

            title = "${query.name} - $claimViewerTitle"

            val parameters = query.toParameters()
            val jViewer = query.viewer.toJsonObject()
            val jFilters = jViewer["filters"] as? JsonArray ?: return

            val filters = mutableMapOf<String, Filter>()
            for (jFilter in jFilters) {
                if (jFilter !is JsonObject) continue
                val jName = jFilter.getString("name", null) ?: continue
                if (parameters.none { it.name == jName }) continue
                val jCaption = jFilter.getString("caption", jName)
                val jCodes = jFilter.getString("codes", null)

                val filter = if (jCodes != null)
                    CodesFilter(jName, jCodes, jCaption) else
                    TextFilter(jName, jCaption)
                filters[filter.name] = filter
                content.addComponent(filter.component)
            }

            val find = Button("Find").apply {
                addStyleName(ValoTheme.BUTTON_PRIMARY)
                setClickShortcut(ShortcutAction.KeyCode.ENTER)
                addClickListener {
                    val src = "/$clientName/query/$path?" + parameters
                        .mapNotNull { parameter -> filters[parameter.name]?.value?.let { parameter.name to it } }
                        .joinToString(separator = "&") { (name, value) ->
                            "$name=${URLEncoder.encode(value, "UTF-8")}"
                        }
                    iFrame.source = ExternalResource(Page.getCurrent().location.resolve(URI(src)).toURL())
                }
            }

            content.addComponent(find)
            content.setComponentAlignment(find, Alignment.MIDDLE_RIGHT)
        }
    }

    @WebServlet(urlPatterns = ["/cv/*"], name = "ClaimViewerServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = ClaimViewerUI::class, productionMode = false)
    class MyUIServlet : VaadinServlet()
}

