package com.simagis.claims.web.ui

import com.vaadin.event.ShortcutAction
import com.vaadin.server.Sizeable
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