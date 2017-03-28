package com.simagis.claims.web.ui

import com.vaadin.data.BinderValidationStatus
import com.vaadin.event.ShortcutAction
import com.vaadin.server.Page
import com.vaadin.server.Sizeable
import com.vaadin.shared.Position
import com.vaadin.shared.ui.MarginInfo
import com.vaadin.ui.*
import com.vaadin.ui.themes.ValoTheme

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/24/2017.
 */
enum class ConfirmationActionType {
    PRIMARY, FRIENDLY, DANGER,
}

fun showConfirmationDialog(
        caption: String,
        message: String = "",
        actionCaption: String = "Confirm",
        actionType: ConfirmationActionType = ConfirmationActionType.DANGER,
        cancelCaption: String = "Cancel",
        body: Component? = null,
        action: (Window) -> Boolean
) = UI.getCurrent().addWindow(
        Window(caption).also { dialog ->
            dialog.content = VerticalLayout().also { main ->
                if (message.isNotBlank()) {
                    Label().apply {
                        main.addComponent(this)
                        styleName = ValoTheme.LABEL_LARGE
                        value = message
                    }
                }
                body?.apply {
                    main.addComponent(this)
                }
                HorizontalLayout().apply {
                    main.addComponent(this)
                    main.setComponentAlignment(this, Alignment.MIDDLE_RIGHT)
                    setMargin(false)
                }.also { buttons ->
                    Button(actionCaption).apply {
                        buttons.addComponent(this)
                        when(actionType) {
                            ConfirmationActionType.PRIMARY -> {
                                addStyleName(ValoTheme.BUTTON_PRIMARY)
                                setClickShortcut(ShortcutAction.KeyCode.ENTER)
                            }
                            ConfirmationActionType.FRIENDLY -> {
                                addStyleName(ValoTheme.BUTTON_FRIENDLY)
                                setClickShortcut(ShortcutAction.KeyCode.ENTER)
                            }
                            ConfirmationActionType.DANGER -> {
                                addStyleName(ValoTheme.BUTTON_DANGER)
                            }
                        }

                        addClickListener {
                            if (action(dialog))
                                dialog.close()
                        }
                    }

                    Button(cancelCaption).apply {
                        buttons.addComponent(this)
                        addClickListener { dialog.close() }
                    }
                }
            }
            dialog.isResizable = false
            dialog.isModal = true
            dialog.setWidth(50f, Sizeable.Unit.PERCENTAGE)
        })

object Margins {
    val ON = MarginInfo(true)
    val OFF = MarginInfo(false)
}

@Suppress("NOTHING_TO_INLINE")
inline fun margins(enabled: Boolean = false): MarginInfo
        = if (enabled) Margins.ON else Margins.OFF

@Suppress("NOTHING_TO_INLINE")
inline fun margins(
        top: Boolean = false,
        right: Boolean = false,
        bottom: Boolean = false,
        left: Boolean = false): MarginInfo
        = MarginInfo(top, right, bottom, left)

data class SizeableK1(val unit: Sizeable.Unit, val value: Float)

val size100pc = SizeableK1(Sizeable.Unit.PERCENTAGE, 100f)

@Suppress("NOTHING_TO_INLINE")
inline fun sizePc(value: Float): SizeableK1 = SizeableK1(Sizeable.Unit.PERCENTAGE, value)

@Suppress("NOTHING_TO_INLINE")
inline fun sizeEm(value: Float): SizeableK1 = SizeableK1(Sizeable.Unit.EM, value)

@Suppress("NOTHING_TO_INLINE")
inline fun sizePx(value: Float): SizeableK1 = SizeableK1(Sizeable.Unit.PIXELS, value)

@Suppress("NOTHING_TO_INLINE")
inline fun sizePt(value: Float): SizeableK1 = SizeableK1(Sizeable.Unit.POINTS, value)

inline var Component.widthK1: SizeableK1
    get() = SizeableK1(widthUnits, width)
    set(value) {
        setWidth(value.value, value.unit)
    }

inline var Component.heightK1: SizeableK1
    get() = SizeableK1(heightUnits, height)
    set(value) {
        setHeight(value.value, value.unit)
    }

fun showError(caption: String, description: String? = null) {
    Notification(caption, description, Notification.Type.WARNING_MESSAGE).apply {
        position = Position.TOP_CENTER
        show(Page.getCurrent())
    }
}

fun <BEAN> BinderValidationStatus<BEAN>.showError(caption: String) = showError(
        caption = caption,
        description = validationErrors.joinToString(separator = "\n") { it.errorMessage })

inline fun <BEAN> BinderValidationStatus<BEAN>.onError(function: (BinderValidationStatus<BEAN>) -> Unit) {
    if (!isOk) function(this)
}
