(function ($) {
        var SubTaskCard = function(ele, opt) {

        };
        SubTaskCard.prototype = {
            init: function(options) {
                this.$root = options.ele;
                this.selectData = options.data;
                this.$ele = this.createView();
                this.append();
                this.attachEvent();
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
            attachPicker: function() {
                this.attachPickerConnection(this.selectData[0]);
                this.attachPickerTable(this.selectData[1]);
                this.attachPickerColumn(this.selectData[2]);
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
                    var $subFieldLi = $(self.subFieldLiTemplate());
                    $(ev.target).parents('.target_field_wrap').find('.target_field_column_wrap').append($subFieldLi);
                    
                    $subFieldLi.find('select').selectpicker('initSelectOption', {
                        idKey: 'value',
                        nameKey: 'label',
                        data: self.selectData[2]
                    });
                });
            }
        };
        $.fn.subTaskCard = function(options) {
            var taskCard = new SubTaskCard(this, options);
            taskCard.init(options);
            return taskCard;
        }

})(jQuery);