from mineru.utils.pdf_classify import _get_page_object_bounds


def test_get_page_object_bounds_uses_current_pdfium_api():
    class PageObject:
        def get_bounds(self):
            return (1.0, 2.0, 3.0, 4.0)

    assert _get_page_object_bounds(PageObject()) == (1.0, 2.0, 3.0, 4.0)


def test_get_page_object_bounds_supports_legacy_pdfium_api():
    class PageObject:
        def get_pos(self):
            return (5.0, 6.0, 7.0, 8.0)

    assert _get_page_object_bounds(PageObject()) == (5.0, 6.0, 7.0, 8.0)
