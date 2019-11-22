(function ($) {
        var SubTaskCard = function(ele, opt) {

        };
        SubTaskCard.prototype = {
            init: function(options) {
                this.getConnectionsUrl = 'http://192.168.100.133:8080/datax/api/connections';
                this.baseUrl = 'http://192.168.100.133:8080/datax/api/connection/';
                this.connectionsVal = '';
                this.connectionsData = [];
                this.tablesData = [];
                this.columnsData = [];
                this.$root = options.ele;
                this.task = options.task;
                this.getConnectionsData();
                this.createFullView();
                this.initChange();
            },
            initChange: function() {
                var self = this;
                this.$ele.find('select.target_field_connection').on('changed.bs.select', function(e) {
                    self.connectionsVal = e.target.value;
                    var url = self.baseUrl + self.connectionsVal + '/tables';
                    self.getTablesData(url);
                });
                this.$ele.find('select.target_field_table').on('changed.bs.select', function(e) {
                    var value = e.target.value;
                    var url = self.baseUrl + self.connectionsVal + '/table/' + value + '/columns';
                    self.getColumnsData(url);
                });
            },
            getConnectionsData: function() {
                var self = this;
                $.ajax({
                    url: self.getConnectionsUrl,
                    cache: false,
                    success: function(result){
                        if (result.code === 0) {
                            self.connectionsData = self.formatData(result.connections);
                            self.attachPickerConnection(self.connectionsData);
                            if (self.task) {
                                self.setRowConnectionValue(self.task);
                            }
                        } else {
                            alert(result.msg);
                        }
                    },
                    error: function() {
                        alert('请求失败，请刷新页面重试');
                    }
                });
            },
            getTablesData: function(url) {
                var self = this;
                $.ajax({
                    url: url,
                    cache: false,
                    success: function(result){
                        if (result.code === 0) {
                            self.tablesData = self.formatData(result.tables);
                            self.attachPickerTable(self.tablesData);
                            if (self.task) {
                                self.setRowTableValue(self.task);
                            }
                        } else {
                            alert(result.msg);
                        }
                    },
                    error: function() {
                        alert('请求失败，请刷新页面重试');
                    }
                });
            },
            getColumnsData: function(url) {
                var self = this;
                $.ajax({
                    url: url,
                    cache: false,
                    success: function(result){
                        if (result.code === 0) {
                            self.columnsData = self.formatColumnsData(result.tables);
                            self.attachPickerColumn(self.columnsData);
                            if (self.task) {
                                self.setRowColumnValue(self.task);
                            }
                        } else {
                            alert(result.msg);
                        }
                    },
                    error: function() {
                        alert('请求失败，请刷新页面重试');
                    }
                });
            },
            formatData: function(data) {
                var formatedData = [];
                $.each(data, function(index, item) {
                    formatedData.push({label: item, value: item});
                });
                return formatedData;
            },
            formatColumnsData: function(data) {
                var formatedData = [];
                $.each(data, function(index, item) {
                    formatedData.push({label: item.column_name, value: item.column_name});
                });
                return formatedData;
            },
            createFullView: function() {
                this.$ele = this.createView();
                this.append();
                this.attachEvent();
                this.attachEmptyPicker();
                if (this.task) {
                    this.setRowStaticValue(this.task);
                }
            },
            template: function() {
                return '<li class="subtask_list_card">' +
                '<div class="subtask_list_card_title">' +
                    '<h4>+    子任务</h4>' +
                    '<span class="glyphicon glyphicon-remove remove_sub_task"></span>' +
                '</div>' +
                '<div>' +
                    '<ul class="subtask_list_card_nav">' +
                        '<li class="li_label">' +
                            '<label class="label_left">子任务名称</label>' +
                            '<input type="text" class="head_input form-control sub_task_name">' +
                        '</li>' +
                        '<li class="li_label">' +
                            '<label class="label_left">前置任务</label>' +
                            '<select name="state" class="head_input selectpicker pre_task_select"></select>' +
                        '</li>' +
                        '<li class="li_label"></li>' +
                    '</ul>' +
                    '<ul class="subtask_list_content">' +
                        '<li class="subtask_list_item">' +
                            '<ul class="nav nav-tabs">' +
                                '<li role="presentation" class="active">' +
                                    '<a data-toggle="tab" disable_anchor="true" role="tab" aria-expanded="true">输入SQL</a>' +
                                '</li>' +
                            '</ul>' +
                            '<div class="tab-content nav nav-tabs">' +
                                '<div role="tabpanel" class="tab-pane active input_sql_wrapper">' +
                                    '<select name="state" class="head_input selectpicker bs-select-hidden sql_input_select"></select>' +
                                    '<textarea class="form-control sql_input_area" rows="8" name="textarea"></textarea>' +
                                '</div>' +
                            '</div>' +
                        '</li>' +
                        '<li class="subtask_list_item">' +
                            '<ul class="nav nav-tabs">' +
                                '<li role="presentation" class="active">' +
                                    '<a data-toggle="tab" disable_anchor="true" role="tab" aria-expanded="true">目的库</a>' +
                                '</li>' +
                            '</ul>' +
                            '<div class="tab-content nav nav-tabs">' +
                                '<div role="tabpanel" class="tab-pane active target_field_wrap">' +
                                    '<ul class="sub_select_zone">' +
                                        '<li class="sub_select_li li_left">' +
                                            '<select name="state" class="head_input selectpicker target_field_connection"></select>' +
                                        '</li>' +
                                        '<li class="sub_select_li li_center">' +
                                            '<select name="state" class="head_input selectpicker target_field_table"></select>' +
                                        '</li>' +
                                        '<li class="sub_select_li li_right">' +
                                            '<ul class="field_wrapper target_field_column_wrap">' +
                                                '<li class="field_li">' +
                                                    '<select name="state" class="head_input selectpicker"></select>' +
                                                    '<span class="glyphicon glyphicon-remove remove_field_li"></span>' +
                                                '</li>' +
                                                '<li class="field_li">' +
                                                    '<select name="state" class="head_input selectpicker"></select>' +
                                                    '<span class="glyphicon glyphicon-remove remove_field_li"></span>' +
                                                '</li>' +
                                                '<li class="field_li">' +
                                                    '<select name="state" class="head_input selectpicker"></select>' +
                                                    '<span class="glyphicon glyphicon-remove remove_field_li"></span>' +
                                                '</li>' +
                                            '</ul>' +
                                        '</li>' +
                                    '</ul>' +
                                    '<button class="btn btn-primary add_new_field">+添加新字段</button>' +
                                '</div>' +
                            '</div>' +
                        '</li>' +
                    '</ul>' +
                '</div>' +
            '</li>';
            },
            subFieldLiTemplate: function() {
                return '<li class="field_li">' +
                    '<select name="state" class="head_input selectpicker"></select>' +
                    '<span class="glyphicon glyphicon-remove remove_field_li"></span>' +
                '</li>';
            },
            createView: function() {
                return $(this.template());
            },
            append: function() {
                this.$root.append(this.$ele);
            },
            attachEvent: function() {
                this.attachClickEvent();
            },
            attachEmptyPicker: function() {
                this.attachPickerConnection([]);
                this.attachPickerTable([]);
                this.attachPickerColumn([]);
            },
            attachPickerConnection: function(data) {
                this.$ele.find('.sql_input_select').selectpicker('initSelectOption', {
                    idKey: 'value',
                    nameKey: 'label',
                    data: data
                });
                this.$ele.find('.target_field_connection').selectpicker('initSelectOption', {
                    idKey: 'value',
                    nameKey: 'label',
                    data: data
                });
            },
            attachPickerTable: function(data){
                this.$ele.find('.target_field_table').selectpicker('initSelectOption', {
                    idKey: 'value',
                    nameKey: 'label',
                    data: data
                });
            },
            attachPickerColumn: function(data) {
                $.each(this.$ele.find('.target_field_column_wrap select'), function(index, dom){
                    $(dom).selectpicker('initSelectOption', {
                        idKey: 'value',
                        nameKey: 'label',
                        data: data
                    });
                });
            },
            attachClickEvent: function() {
                var self = this;
                this.$ele.find('.remove_sub_task').click(function(ev) {
                    $(ev.target).parents('.subtask_list_card').remove();
                });
                this.$ele.on('click', '.remove_field_li', function(ev) {
                    $(ev.target).parents('.field_li').remove();
                });
                this.$ele.find('.add_new_field').click(function(ev){
                    var newFieldWrap = $(ev.target).parents('.target_field_wrap').find('.target_field_column_wrap');
                    self.addNewField(newFieldWrap);
                });
            },
            addNewField: function ($wrap){
                var self = this;
                var $subFieldLi = $(self.subFieldLiTemplate());
                $wrap.append($subFieldLi);
                
                $subFieldLi.find('select').selectpicker('initSelectOption', {
                    idKey: 'value',
                    nameKey: 'label',
                    data: self.columnsData
                });
            },
            setRowStaticValue: function(rowValue) {
                var $row = this.$ele;
                $row.find('.sub_task_name').val(rowValue.name);
                $row.find('.sql_input_area').val(rowValue.source.query_sql);
            },
            setRowPreTaskValue: function() {
                var $row = this.$ele;
                $row.find('.pre_task_select').selectpicker('val', rowValue.pre_task);
            },
            setRowConnectionValue: function(rowValue) {
                var $row = this.$ele;
                $row.find('.sql_input_select').selectpicker('val', rowValue.source.conn_id);
                $row.find('.target_field_connection').selectpicker('val', rowValue.target.conn_id);
                this.connectionsVal = rowValue.target.conn_id;
                var url = this.baseUrl + this.connectionsVal + '/tables';
                this.getTablesData(url);
            },
            setRowTableValue: function(rowValue) {
                var $row = this.$ele;
                $row.find('.target_field_table').selectpicker('val', rowValue.target.table);
                var value = rowValue.target.table;
                var url = this.baseUrl + this.connectionsVal + '/table/' + value + '/columns';
                this.getColumnsData(url);
            },
            setRowColumnValue: function(rowValue) {
                var $row = this.$ele;
                var $columns = $row.find('.target_field_column_wrap .selectpicker');
                var $columnWrap = $row.find('.target_field_column_wrap');
                var columns = rowValue.target.columns;
                if (!columns.length) {
                    $columnWrap.empty();
                } else if (columns.length > $columns.length) {
                    for (var i = 0; i < columns.length - $columns.length; i++) {
                        this.addNewField($columnWrap);
                    }
                }
                $.each($row.find('.target_field_column_wrap .field_li'), function(index, dom){
                    var columnVal = rowValue.target.columns[index];
                    var $dom = $(dom);
                    if (columnVal) {
                        $dom.find('.selectpicker').selectpicker('val', columnVal);
                    } else {
                        $dom.remove();
                    }
                });
            },
        };
        $.fn.subTaskCard = function(options) {
            var taskCard = new SubTaskCard(this, options);
            taskCard.init(options);
            return taskCard;
        }

})(jQuery);
