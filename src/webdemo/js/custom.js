$(document).ready(function() {
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
		}
	});

	showCorrectRadioDiv();
});

function showCorrectRadioDiv() {
	$("#input-radio").find('.radio').siblings().find('.form-radio').hide();
	$("#input-radio").find('.radio input:checked').closest('.radio').find('.form-radio').show();
}