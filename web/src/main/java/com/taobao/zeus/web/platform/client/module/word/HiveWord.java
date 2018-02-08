package com.taobao.zeus.web.platform.client.module.word;

import com.google.gwt.event.logical.shared.BeforeSelectionEvent;
import com.google.gwt.event.logical.shared.BeforeSelectionHandler;
import com.google.gwt.user.client.ui.Widget;
import com.sencha.gxt.widget.core.client.TabItemConfig;
import com.taobao.zeus.web.platform.client.lib.codemirror.CodeMirror;
import com.taobao.zeus.web.platform.client.module.filemanager.FileModel;
import com.taobao.zeus.web.platform.client.module.word.component.EditTab;
import com.taobao.zeus.web.platform.client.module.word.component.HistoryTab;
import com.taobao.zeus.web.platform.client.module.word.component.ViewTab;
import com.taobao.zeus.web.platform.client.util.PlatformContext;

public class HiveWord extends Word {

	public HiveWord(PlatformContext context, WordPresenter presenter,
			FileModel fmodel) {
		super(context, presenter);
		this.model = fmodel;

		if (fmodel.isAdmin() || context.getUser().isSuper()) {
			add(getEditTab(), new TabItemConfig("编辑", false));
			add(getHistoryTab(), new TabItemConfig("调试历史", false));
			setActiveWidget(editTab);
		} else {
			add(getViewTab(), new TabItemConfig("查看", false));
		}
		addBeforeSelectionHandler(new BeforeSelectionHandler<Widget>() {
			@Override
			public void onBeforeSelection(BeforeSelectionEvent<Widget> event) {
				if (event.getItem() == getViewTab()) {
					if (editTab.getNewContent() != null) {
						getViewTab().refresh(editTab.getNewContent());
					} else if (model.getContent() != null) {
						getViewTab().refresh(model.getContent());
					}
				} else if (event.getItem() == getEditTab()) {
					getEditTab().getCodeMirror().refresh();
				} else if (event.getItem() == getHistoryTab()) {
					getHistoryTab().refresh();
				}
			}
		});
	}

	private ViewTab viewTab;
	private EditTab editTab;
	private HistoryTab historyTab;

	protected ViewTab getViewTab() {
		if (viewTab == null) {
			viewTab = new ViewTab(model);
		}
		return viewTab;
	}

	public EditTab getEditTab() {
		if (editTab == null) {
			editTab = new EditTab(context, presenter, model);
		}
		return editTab;
	}

	protected HistoryTab getHistoryTab() {
		if (historyTab == null) {
			historyTab = new HistoryTab(context, presenter, model);
		}
		return historyTab;
	}


	@Override
	protected CodeMirror getCodeMirror() {
		return getEditTab().getCodeMirror();
	}

}
