$(document).ready(function() {

	$("#myModal2 a").on('click', function() {
		$("#commandsArea").val($(this).text());
		$('#commandsArea').trigger("change");

		// Bad but it fixes the scrollbar problem
		// http://stackoverflow.com/questions/10466129/how-to-hide-bootstrap-modal-from-javascript
		$("#myModal2 [data-dismiss=modal]").trigger({ type: "click" });

		return false;
	});

	$("#check_showResults").on('change', function() {
		var isChecked = $("#check_showResults").is(":checked");
		if (isChecked) {
			$("#fileCompression").attr("disabled", "disabled");
			$("#fileCompression").val("");
		}
		else {
			$("#fileCompression").removeAttr("disabled");
		}
	});

	$("#input-radio input:radio").on('change', function() {
		showCorrectRadioDiv();
	});

	$(".cancel-file").on('click', function() {
		// See: http://stackoverflow.com/questions/1043957/clearing-input-type-file-using-jquery
		$(this).closest('.form-group').find('input').wrap('<form>').closest('form').get(0).reset();
		$(this).closest('.form-group').find('input').unwrap();
		return false;
	});

	$("#show-hide-button").on('click', function() {
		if ($("#documentation pre").is(":visible")) {
			$("#documentation pre").hide();

			$("#form-components").removeClass();
			$("#form-components").addClass("col-md-11");
			$("#documentation").removeClass();
			$("#documentation").addClass("col-md-1");

			$("#show-hide-button-text").hide();
			$("#show-hide-button .glyphicon").removeClass("glyphicon-resize-small");
			$("#show-hide-button .glyphicon").addClass("glyphicon-resize-full");

			$("#documentation .btn-hide").hide();
		}
		else {
			$("#documentation pre").show();

			$("#form-components").removeClass();
			$("#form-components").addClass("col-md-5");
			$("#documentation").removeClass();
			$("#documentation").addClass("col-md-7");

			$("#show-hide-button-text").show();
			$("#show-hide-button .glyphicon").removeClass("glyphicon-resize-full");
			$("#show-hide-button .glyphicon").addClass("glyphicon-resize-small");

			$("#documentation .btn-hide").show();
		}
	});

	// $("#exampleList").on('change', function() {
	// 	var val = $(this).val();
	// 	if (val != "") {
	// 		$('#commandsArea').val(val);
	// 		$('#commandsArea').trigger("change");
	// 	}
	// 	return false;
	// });

	$('#commandsArea').textcomplete([
	    { // commands
	        mentions: allowedCommands,
	        match: /\B@(\w*)$/,
	        search: function (term, callback) {
	            callback($.map(this.mentions, function (mention) {
	                return mention.indexOf(term) === 0 ? mention : null;
	            }));
	        },
	        index: 1,
	        replace: function (mention) {
	            return '@' + mention + ' ';
	        }
	    },
	    { // files
	        mentions: fileList,
	        match: /\B#(\w*)$/,
	        search: function (term, callback) {
	            callback($.map(this.mentions, function (mention) {
	                return mention.indexOf(term) === 0 ? mention : null;
	            }));
	        },
	        index: 1,
	        replace: function (mention) {
	            return '#' + mention + ' ';
	        }
	    }
	]);

	$('#commandsArea').overlay([
		{
	        match: /\B@\w+/g,
	        css: {
	            'background-color': '#d8dfea'
	        }
	    },
		{
	        match: /\B#\w+/g,
	        css: {
	            'background-color': '#ffdd77'
	        }
	    }
	]);

	showCorrectRadioDiv();
});

function showCorrectRadioDiv() {
	$("#input-radio").find('.radio').siblings().find('.form-radio').hide();
	$("#input-radio").find('.radio input:checked').closest('.radio').find('.form-radio').show();
}